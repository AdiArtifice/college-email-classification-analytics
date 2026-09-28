#!/usr/bin/env python3
"""
parse_mbox.py — Phase 1: MBOX → JSONL + Parquet
Intelligent College Email Classification and Opportunity Analytics System
BDA Mini-Project

Reads a Google Takeout .mbox export in streaming fashion, deduplicates
by Message-ID, decodes all RFC-2047 encoded headers, extracts body text
(plain-text preferred, html2text fallback for HTML-only), and writes
two output files consumed by downstream pipeline stages:

  <output>/structured/emails.jsonl   →  uploaded to HDFS raw/structured/
                                         consumed by Java MapReduce (TextInputFormat)
  <output>/cleaned/emails.parquet    →  uploaded to HDFS cleaned/
                                         consumed by PySpark (columnar read)
  <output>/parse_summary.json        →  validation statistics

Schema matches implementation_plan.md §4 (clean layer):
  message_id, thread_id, sender, sender_email, sender_domain,
  recipients, subject, date_utc, body_text, body_length,
  has_attachment, attachment_count, gmail_labels,
  is_unread, is_spam, is_sent, is_important

Usage:
    python3 parser/parse_mbox.py \\
        --mbox "metadata/All mail Including Spam and Trash.mbox" \\
        --output data/output \\
        [--sample 10]
"""

import sys
import os
import json
import mailbox
import logging
import argparse
import re
from datetime import timezone
from email import header as email_header
from email.utils import parseaddr, parsedate_to_datetime, getaddresses

import html2text
import pyarrow as pa
import pyarrow.parquet as pq


# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Header / address utilities ───────────────────────────────────────────────

def decode_header_value(raw: str | None) -> str:
    """
    Decode an RFC-2047 encoded header value to plain Unicode text.

    Handles:
    - Base64 encoded words:  =?utf-8?B?...?=
    - Quoted-printable words: =?utf-8?Q?...?=
    - Multi-segment encoded subjects split across two tokens
    - Mixed charset parts in a single header
    - Malformed / partially encoded values (falls back gracefully)

    Always uses errors='replace' so no crash on bad bytes.
    """
    if not raw:
        return ""
    try:
        parts = email_header.decode_header(raw)
        segments = []
        for part, charset in parts:
            if isinstance(part, bytes):
                segments.append(part.decode(charset or "utf-8", errors="replace"))
            else:
                segments.append(part)
        return "".join(segments).strip()
    except Exception:
        # Strip encoded-word markers as a last resort and return raw text
        return re.sub(r"=\?[^?]+\?[bqBQ]\?[^?]*\?=", "", raw or "").strip()


def decode_addresses(raw: str | None) -> list[str]:
    """
    Parse a To/Cc header into a list of normalised address strings.
    Decodes RFC-2047 encoded display names before parsing.
    Returns an empty list on any error (never crashes).
    """
    if not raw:
        return []
    try:
        # Decode the entire header value first (handles encoded display names)
        decoded = decode_header_value(raw)
        pairs = getaddresses([decoded])
        result = []
        for name, addr in pairs:
            name = name.strip()
            addr = addr.lower().strip()
            if addr:
                result.append(f"{name} <{addr}>" if name else addr)
        return result
    except Exception:
        return []


def parse_gmail_labels(raw: str | None) -> list[str]:
    """
    Parse the X-Gmail-Labels header into a clean list of label strings.

    Handles two known real-world issues from this dataset:
    1. MIME header line-folding:  'Category\\n updates' → 'Category updates'
       (Google Takeout wraps long label lines at 76 chars with CRLF + whitespace)
    2. Embedded spaces within multi-word label names such as
       'Category promotions', 'Category updates', 'Category forums', etc.

    Strategy: collapse all runs of whitespace to a single space BEFORE
    splitting on comma, so folded continuation lines merge back into
    one label string rather than creating spurious blank labels.
    """
    if not raw:
        return []
    collapsed = re.sub(r"\s+", " ", raw).strip()
    return [lbl.strip() for lbl in collapsed.split(",") if lbl.strip()]


def normalize_date(raw: str | None) -> str | None:
    """
    Parse an email Date header → ISO-8601 UTC string (e.g. '2025-03-15T08:30:00+00:00').
    Returns None if the date cannot be reliably parsed.
    Never guesses; downstream Spark stage can fill nulls if needed.
    """
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def extract_thread_id(msg) -> str | None:
    """
    Best-effort thread linkage:
    1. Prefer In-Reply-To (direct parent Message-ID)
    2. Fall back to the first entry in References (root of thread)
    3. Return None if this is a standalone (root) message
    """
    in_reply_to = (msg.get("In-Reply-To") or "").strip()
    if in_reply_to:
        return in_reply_to

    refs_raw = (msg.get("References") or "").split()
    if refs_raw:
        return refs_raw[0]          # oldest ancestor = thread root

    return None


# ── HTML → plain text converter (module-level singleton) ────────────────────

_h2t = html2text.HTML2Text()
_h2t.ignore_links        = True    # drop hyperlink URLs from output
_h2t.ignore_images       = True    # drop image references
_h2t.ignore_emphasis     = True    # drop markdown-style *bold* markers
_h2t.body_width          = 0       # disable line-wrapping
_h2t.unicode_snob        = True    # prefer real unicode over ASCII approximations
_h2t.skip_internal_links = True


def _decode_payload(part, fallback_charset: str = "utf-8") -> str:
    """
    Decode a single message part's payload bytes to a string.
    Always safe: uses errors='replace' with a best-guess charset.
    """
    raw = part.get_payload(decode=True)
    if not raw:
        return ""
    charset = (part.get_content_charset() or fallback_charset).strip()
    if not charset:
        charset = "utf-8"
    return raw.decode(charset, errors="replace")


def extract_body(msg) -> tuple[str, bool, int]:
    """
    Extract the human-readable text body from a message.

    Priority order:
      1. First non-attachment text/plain part  (preferred — no conversion needed)
      2. html2text of first text/html part      (fallback for HTML-only senders)
      3. Empty string                           (no readable content found)

    Attachments are counted but their binary content is never read into text.

    Returns:
        body_text        : str  — extracted/converted text (may be empty string)
        used_html        : bool — True when HTML fallback was used
        attachment_count : int  — number of attachment parts encountered
    """
    plain_parts: list[str] = []
    html_parts:  list[str] = []
    attachment_count = 0

    if msg.is_multipart():
        for part in msg.walk():
            ctype       = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "").lower()
            filename    = part.get_filename()

            # Identify attachments by explicit Content-Disposition or presence of a filename
            if "attachment" in disposition or filename:
                attachment_count += 1
                continue

            if ctype == "text/plain":
                text = _decode_payload(part)
                if text:
                    plain_parts.append(text)

            elif ctype == "text/html" and not plain_parts:
                # Collect HTML only when no plain text has been found yet
                html_text = _decode_payload(part)
                if html_text:
                    html_parts.append(html_text)
    else:
        # Non-multipart message: read the single payload directly
        ctype = msg.get_content_type()
        text  = _decode_payload(msg)
        if ctype == "text/plain":
            plain_parts.append(text)
        elif ctype == "text/html":
            html_parts.append(text)

    # ── Return best available body ────────────────────────────────────────
    if plain_parts:
        return "\n".join(plain_parts), False, attachment_count

    if html_parts:
        try:
            converted = _h2t.handle("\n".join(html_parts))
            return converted, True, attachment_count
        except Exception:
            return "", True, attachment_count

    return "", False, attachment_count


# ── PyArrow schema — matches §4 clean-layer schema in implementation_plan.md ─

SCHEMA = pa.schema([
    pa.field("message_id",       pa.string(),           nullable=False),
    pa.field("thread_id",        pa.string(),            nullable=True),
    pa.field("sender",           pa.string(),            nullable=True),
    pa.field("sender_email",     pa.string(),            nullable=True),
    pa.field("sender_domain",    pa.string(),            nullable=True),
    pa.field("recipients",       pa.list_(pa.string()),  nullable=False),
    pa.field("subject",          pa.string(),            nullable=True),
    pa.field("date_utc",         pa.string(),            nullable=True),
    pa.field("body_text",        pa.string(),            nullable=False),
    pa.field("body_length",      pa.int32(),             nullable=False),
    pa.field("has_attachment",   pa.bool_(),             nullable=False),
    pa.field("attachment_count", pa.int32(),             nullable=False),
    pa.field("gmail_labels",     pa.list_(pa.string()),  nullable=False),
    pa.field("is_unread",        pa.bool_(),             nullable=False),
    pa.field("is_spam",          pa.bool_(),             nullable=False),
    pa.field("is_sent",          pa.bool_(),             nullable=False),
    pa.field("is_important",     pa.bool_(),             nullable=False),
])

# Number of records accumulated before flushing a Parquet row group.
# Lower = less peak memory; higher = better Parquet compression.
_PARQUET_BATCH_SIZE = 500


# ── Core parse function ───────────────────────────────────────────────────────

def parse_mbox(mbox_path: str, output_dir: str) -> tuple[dict, str, str]:
    """
    Stream-parse <mbox_path>, deduplicate by Message-ID, and write:
      - JSONL   → <output_dir>/structured/emails.jsonl
      - Parquet → <output_dir>/cleaned/emails.parquet

    Processes the MBOX in O(1) memory — one message at a time — using
    Python's mailbox.mbox iterator (no full-file read).

    Returns:
        stats       : dict  — parsing statistics for validation & reporting
        jsonl_path  : str   — path to written JSONL file
        parquet_path: str   — path to written Parquet file
    """
    structured_dir = os.path.join(output_dir, "structured")
    cleaned_dir    = os.path.join(output_dir, "cleaned")
    os.makedirs(structured_dir, exist_ok=True)
    os.makedirs(cleaned_dir,    exist_ok=True)

    jsonl_path   = os.path.join(structured_dir, "emails.jsonl")
    parquet_path = os.path.join(cleaned_dir,    "emails.parquet")

    stats = {
        "total_raw":        0,   # total messages encountered in mbox (including dups)
        "skipped_no_id":    0,   # messages with no Message-ID header
        "skipped_dup":      0,   # duplicate Message-IDs (same email under multiple labels)
        "written":          0,   # unique records written
        "html_fallback":    0,   # bodies converted from HTML (no plain text found)
        "empty_body":       0,   # messages with no extractable body text
        "missing_date":     0,   # messages with unparseable / absent Date header
        "missing_subject":  0,   # messages with no Subject header
        "with_attachments": 0,   # messages that have at least one attachment
        "is_unread":        0,
        "is_spam":          0,
        "is_sent":          0,
        "is_important":     0,
    }

    seen_ids: set[str] = set()       # Message-IDs already written (dedup set)
    batch:    list[dict] = []        # accumulate rows for Parquet batch writes
    pq_writer: pq.ParquetWriter | None = None

    def _flush(rows: list[dict]) -> None:
        """Write a batch of record dicts to the Parquet file."""
        nonlocal pq_writer
        if not rows:
            return
        cols = {field.name: [] for field in SCHEMA}
        for rec in rows:
            for field in SCHEMA:
                cols[field.name].append(rec[field.name])
        table = pa.table(cols, schema=SCHEMA)
        if pq_writer is None:
            pq_writer = pq.ParquetWriter(
                parquet_path, SCHEMA, compression="snappy"
            )
        pq_writer.write_table(table)

    mbox_obj = mailbox.mbox(mbox_path, create=False)

    try:
        with open(jsonl_path, "w", encoding="utf-8") as jf:

            for msg in mbox_obj:
                stats["total_raw"] += 1

                # ── Progress heartbeat every 500 raw messages ─────────────
                if stats["total_raw"] % 500 == 0:
                    log.info(
                        "  raw=%-6d  written=%-6d  dups=%-5d  no-id=%-4d",
                        stats["total_raw"], stats["written"],
                        stats["skipped_dup"], stats["skipped_no_id"],
                    )

                # ── 1. Dedup by Message-ID ────────────────────────────────
                msg_id = (msg.get("Message-ID") or "").strip()
                if not msg_id:
                    stats["skipped_no_id"] += 1
                    continue
                if msg_id in seen_ids:
                    stats["skipped_dup"] += 1
                    continue
                seen_ids.add(msg_id)

                # ── 2. Thread linkage ─────────────────────────────────────
                thread_id = extract_thread_id(msg)

                # ── 3. Sender ─────────────────────────────────────────────
                from_raw     = decode_header_value(msg.get("From") or "")
                sender_name, sender_email = parseaddr(from_raw)
                sender_name  = sender_name.strip()
                sender_email = sender_email.lower().strip() if sender_email else ""
                sender_domain = (
                    sender_email.split("@", 1)[1] if "@" in sender_email else ""
                )

                # ── 4. Recipients (To + Cc) ───────────────────────────────
                recipients = (
                    decode_addresses(msg.get("To"))
                    + decode_addresses(msg.get("Cc"))
                )

                # ── 5. Subject ────────────────────────────────────────────
                subject = decode_header_value(msg.get("Subject")) or None
                if not subject:
                    stats["missing_subject"] += 1

                # ── 6. Date → UTC ─────────────────────────────────────────
                date_utc = normalize_date(msg.get("Date"))
                if not date_utc:
                    stats["missing_date"] += 1

                # ── 7. Gmail labels & boolean flags ───────────────────────
                labels      = parse_gmail_labels(msg.get("X-Gmail-Labels"))
                is_unread   = "Unread"    in labels
                is_spam     = "Spam"      in labels
                is_sent     = "Sent"      in labels
                is_important= "Important" in labels

                # ── 8. Body extraction ────────────────────────────────────
                body_text, used_html, att_count = extract_body(msg)
                body_text_clean = body_text  # raw body; NLP cleaning in Phase 4

                # ── 9. Update counters ────────────────────────────────────
                if used_html:                stats["html_fallback"]    += 1
                if not body_text.strip():    stats["empty_body"]       += 1
                if att_count > 0:            stats["with_attachments"] += 1
                if is_unread:                stats["is_unread"]        += 1
                if is_spam:                  stats["is_spam"]          += 1
                if is_sent:                  stats["is_sent"]          += 1
                if is_important:             stats["is_important"]     += 1

                # ── 10. Assemble record ───────────────────────────────────
                record: dict = {
                    "message_id":       msg_id,
                    "thread_id":        thread_id,           # str | None
                    "sender":           sender_name or sender_email or None,
                    "sender_email":     sender_email or None,
                    "sender_domain":    sender_domain or None,
                    "recipients":       recipients,          # list[str] (may be [])
                    "subject":          subject,             # str | None
                    "date_utc":         date_utc,            # str | None
                    "body_text":        body_text_clean,     # str (may be "")
                    "body_length":      len(body_text_clean),
                    "has_attachment":   att_count > 0,
                    "attachment_count": att_count,
                    "gmail_labels":     labels,              # list[str] (may be [])
                    "is_unread":        is_unread,
                    "is_spam":          is_spam,
                    "is_sent":          is_sent,
                    "is_important":     is_important,
                }

                # ── 11. Write JSONL (one JSON object per line, UTF-8) ─────
                # ensure_ascii=False: preserve Unicode; Hadoop reads UTF-8 fine
                jf.write(json.dumps(record, ensure_ascii=False) + "\n")

                # ── 12. Accumulate for Parquet batch write ────────────────
                batch.append(record)
                if len(batch) >= _PARQUET_BATCH_SIZE:
                    _flush(batch)
                    batch.clear()

                stats["written"] += 1

            # ── Flush final partial batch ─────────────────────────────────
            if batch:
                _flush(batch)

    finally:
        if pq_writer is not None:
            pq_writer.close()
        mbox_obj.close()

    return stats, jsonl_path, parquet_path


# ── Validation helpers ────────────────────────────────────────────────────────

def print_sample(jsonl_path: str, n: int = 10) -> None:
    """Print n sample records from the JSONL output for visual inspection."""
    print(f"\n{'='*72}")
    print(f"SAMPLE — first {n} records from {os.path.basename(jsonl_path)}")
    print("="*72)
    with open(jsonl_path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            r = json.loads(line)
            print(f"\n--- Record {i+1} ---")
            print(f"  message_id   : {r['message_id'][:80]}")
            print(f"  sender       : {r['sender']}")
            print(f"  sender_domain: {r['sender_domain']}")
            print(f"  date_utc     : {r['date_utc']}")
            print(f"  subject      : {str(r['subject'])[:80]}")
            print(f"  gmail_labels : {r['gmail_labels']}")
            print(f"  flags        : unread={r['is_unread']}  spam={r['is_spam']}"
                  f"  sent={r['is_sent']}  important={r['is_important']}")
            print(f"  attachment   : {r['has_attachment']} ({r['attachment_count']} files)")
            print(f"  body_length  : {r['body_length']:,} chars")
            print(f"  body_preview : {repr(r['body_text'][:120])}")


def validate_parquet(parquet_path: str) -> None:
    """Print structural metadata of the generated Parquet file."""
    print(f"\n{'='*72}")
    print(f"PARQUET VALIDATION — {os.path.basename(parquet_path)}")
    print("="*72)
    pf   = pq.ParquetFile(parquet_path)
    meta = pf.metadata
    print(f"  Total rows   : {meta.num_rows:,}")
    print(f"  Row groups   : {meta.num_row_groups}")
    print(f"  Columns      : {meta.num_columns}")
    print(f"  Schema       : {[f.name for f in pf.schema_arrow]}")
    file_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print(f"  File size    : {file_mb:.2f} MB")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Phase 1 — Parse Google Takeout MBOX → JSONL + Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument(
        "--mbox", required=True,
        help="Path to the Google Takeout .mbox file",
    )
    ap.add_argument(
        "--output", default="data/output",
        help="Root output directory (default: data/output)",
    )
    ap.add_argument(
        "--sample", type=int, default=10,
        help="Number of sample records to display for validation (default: 10, 0=skip)",
    )
    args = ap.parse_args()

    if not os.path.isfile(args.mbox):
        log.error("MBOX not found: %s", args.mbox)
        sys.exit(1)

    mbox_mb = os.path.getsize(args.mbox) / (1024 * 1024)
    log.info("Phase 1 parser starting")
    log.info("  MBOX   : %s  (%.1f MB)", args.mbox, mbox_mb)
    log.info("  Output : %s", os.path.abspath(args.output))
    log.info("  Batch  : %d records per Parquet row-group", _PARQUET_BATCH_SIZE)

    stats, jsonl_path, parquet_path = parse_mbox(args.mbox, args.output)

    # ── Summary report ────────────────────────────────────────────────────
    print(f"\n{'='*72}")
    print("PARSE SUMMARY")
    print("="*72)
    print(f"  Total raw messages (incl. duplicates) : {stats['total_raw']:>7,}")
    print(f"  Skipped — no Message-ID               : {stats['skipped_no_id']:>7,}")
    print(f"  Skipped — duplicate Message-ID        : {stats['skipped_dup']:>7,}")
    print(f"  {'─'*43}")
    print(f"  Written (unique records)              : {stats['written']:>7,}")
    print()
    print(f"  Body stats:")
    print(f"    HTML fallback used                  : {stats['html_fallback']:>7,}")
    print(f"    Empty body                          : {stats['empty_body']:>7,}")
    print(f"  Header stats:")
    print(f"    Missing date                        : {stats['missing_date']:>7,}")
    print(f"    Missing subject                     : {stats['missing_subject']:>7,}")
    print(f"  Attachment stats:")
    print(f"    Messages with attachments           : {stats['with_attachments']:>7,}")
    print(f"  Label stats (non-exclusive):")
    print(f"    Unread                              : {stats['is_unread']:>7,}")
    print(f"    Sent                                : {stats['is_sent']:>7,}")
    print(f"    Spam                                : {stats['is_spam']:>7,}")
    print(f"    Important                           : {stats['is_important']:>7,}")

    # ── Validation threshold check ────────────────────────────────────────
    _THRESHOLD = 7_100
    print()
    if stats["written"] >= _THRESHOLD:
        print(f"  ✓ PASS  {stats['written']:,} unique records >= threshold {_THRESHOLD:,}")
    else:
        print(f"  ✗ WARN  Only {stats['written']:,} unique records — below threshold {_THRESHOLD:,}")
        print("         Inspect log output above for parsing errors.")

    # ── Output file sizes ─────────────────────────────────────────────────
    jsonl_mb   = os.path.getsize(jsonl_path)   / (1024 * 1024)
    parquet_mb = os.path.getsize(parquet_path) / (1024 * 1024)
    print()
    print(f"  Output files:")
    print(f"    JSONL   → {jsonl_path}")
    print(f"             ({jsonl_mb:.1f} MB)")
    print(f"    Parquet → {parquet_path}")
    print(f"             ({parquet_mb:.1f} MB, snappy compressed)")

    # ── Persist summary for audit trail ──────────────────────────────────
    summary = {
        **stats,
        "jsonl_path":       jsonl_path,
        "parquet_path":     parquet_path,
        "jsonl_mb":         round(jsonl_mb, 2),
        "parquet_mb":       round(parquet_mb, 2),
        "threshold":        _THRESHOLD,
        "threshold_passed": stats["written"] >= _THRESHOLD,
    }
    summary_path = os.path.join(args.output, "parse_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"    Summary → {summary_path}")

    # ── Visual validation ─────────────────────────────────────────────────
    if args.sample > 0:
        print_sample(jsonl_path, args.sample)

    validate_parquet(parquet_path)

    print()
    log.info("Phase 1 complete.")


if __name__ == "__main__":
    main()
