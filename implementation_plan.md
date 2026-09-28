# Implementation Plan

**Intelligent College Email Classification and Opportunity Analytics System**

Base: `BDA_Email_Project_Context.md` + dataset analysis of `dataset\All mail Including Spam and Trash.mbox`

---

## 1. Dataset Reality Check (as measured)

| Attribute | Value |
|---|---|
| Raw file | `All mail Including Spam and Trash.mbox` |
| Size | ~1.3 GB (1,299,726,336 bytes) |
| Total messages | **7,287** |
| Unread / Read | 3,641 / 3,583 |
| With attachments | ~585 |
| Missing subject | 76 |
| Spam | 26 · Sent | 322 |
| Date range | Aug 2023 → Sep 2026 |
| Volume by year | 2023:101 · 2024:909 · 2025:3,933 · 2026:2,344 |

**Key sender domains (opportunity signals highlighted):**

| Domain | Meaning | Count |
|---|---|---|
| dare2compete.news | Unstop — hackathons/competitions | 934 |
| sjcem.edu.in | College (academic/admin) | 908 |
| classroom.google.com | Academic coursework | 454 |
| eduskillsfoundation.org | Internships | 219 |
| naukri.com | Jobs/placements | 162 |
| educative.io / coursera.org / roboflow.com / github.com / datacamp.com | Learning / platforms | 300+ |

**Gmail labels already present** (`X-Gmail-Labels`):
`Inbox`, `Unread`, `Opened`, `Important`, `Starred`, `Category updates/promotions/forums/social/personal/purchases/bills`, `Sent`, `Spam`.

**Confirmed data-quality issues (Veracity / parser must handle):**
- RFC 2047 encoded subjects (`=?utf-8?B?...?=`)
- Line-folded / split labels (`Category\n updates`)
- Unicode / non-ASCII chars (cp1252 crashes, zero-width chars)
- `multipart/alternative` + plain-text fallbacks; some HTML-only bodies
- Quoted reply text in threads (noise for NLP)
- Ambiguous / missing dates and deadlines

---

## 2. Feasibility Verdict (critical, honest)

**Suitable for a BDA mini-project: YES**, with an explicit caveat.

- **Volume caveat:** 7.3K emails / 1.3 GB is "medium data", not internet scale. This is normal for a college mini-project and is *defensible* by demonstrating: (a) HDFS partitioning across blocks, (b) Spark partition-level parallelism, (c) the **growth trajectory** (101 → 3,933/year) that justifies a distributed, horizontal-scalable design, and (d) a multi-student vision where each user adds their own corpus.
- **Variety / Velocity / Veracity / Value:** all map convincingly to the measured data (Section 1). Value is strong — Unstop + Naukri + EduSkills + college notices are genuine, extractable opportunity signals.

**Decision: Proceed.** Every downstream step below is chosen to either directly serve the pipeline or to make the architecture *viva-defensible*.

---

## 3. Goals & Non-Goals

### Goals (deliverables)
1. Parse the 1.3 GB MBOX into clean structured records.
2. Store raw + parsed data in HDFS (distributed layering).
3. Demonstrate one meaningful **Java Hadoop MapReduce** batch job.
4. Build the **Spark (PySpark)** processing + classification pipeline.
5. Extract opportunity metadata (organization, deadline, campus type, domain).
6. Produce analytics via **Hive**.
7. Present insights in a **Streamlit** dashboard.
8. Show the **5 Vs** and (optionally) incremental processing.

### Non-goals
- Multi-user production SaaS, OAuth, email send/reply, mobile app.
- Kafka streaming, deep learning / LLM, MongoDB, Elasticsearch (not needed).

---

## 4. Target Schema (finalize during parser build)

**Structured record (`clean` layer):**

```
message_id        (Message-ID, unique key)
thread_id         (In-Reply-To / References, best effort)
sender            (decoded display)
sender_email
sender_domain
recipients        (To + Cc, list)
subject           (decoded)
date_utc          (normalized ISO-8601 UTC)
body_text         (extracted plain text, html fallback)
body_length       (chars)
has_attachment    (bool)
attachment_count  (int)
gmail_labels      (list, parsed from X-Gmail-Labels)
is_unread         (bool)
is_spam           (bool)
is_sent           (bool)
is_important      (bool)
```

**Derived (`feature` layer, added by Spark):**
```
category          (Internship | Job | Interview | Hackathon | Competition |
                   Workshop/Event | Scholarship | Academic | Examination |
                   Administrative | Newsletter/Promotional | Miscellaneous)
campus_type       (On-campus | Off-campus | null)
organization      (extracted, null if unknown)
opportunity_type  (fine-grained sub-domain, e.g. "AI/ML", "Web Dev", "Core Engineering";
                   null unless category is an opportunity class. Distinct from `category`:
                   category = the class itself (Internship), opportunity_type = its domain/topic)
deadline          (ISO date, null if unknown)
event_date        (ISO date, null if unknown)
keywords          (list)
```

---

## 5. Architecture & Data Flow

```
 MBOX (1.3 GB)
      │  [1] Python parser (dedupe by Message-ID, decode, normalize)
      v
      ├──► JSON Lines ──► HDFS raw/structured/
      │
      └──► Parquet     ──► HDFS cleaned/

 HDFS raw/structured/ (JSONL)
      └──► [3] Java MapReduce ── batch aggregates (domain×month counts) ──► HDFS results/

 HDFS cleaned/ (parquet)
      └──► [4] Spark (PySpark):
                   text clean → TF-IDF → classify → extract
                          │
                          v
                     HDFS features/ (parquet)
                          │
                          v
                     [5] Hive external tables → SQL analytics
                          │
                          v
              (Hive queries materialized as small result Parquet/CSV files)
                          │
                          v
                     [6] Streamlit dashboard (reads result files directly via pandas)
```

Every technology has a single clear job (defensible mapping):

| Tech | Job |
|---|---|
| Python `mailbox` parser | MBOX → structured (decoding headers/bodies) |
| HDFS | Distributed storage of raw/cleaned/features |
| Hadoop MapReduce (Java) | One distributed batch aggregation |
| Spark / PySpark | Processing + NLP/ML + info extraction |
| Hive | SQL analytics layer |
| Streamlit | Visualization |

---

## 6. Phased Plan

### Phase 0 — Setup & Tooling (0.5 day)
- Confirm environment: **Python 3.11 or 3.12** (PySpark 3.5.x does not yet support Python 3.13 — pin the venv to 3.11/3.12), Java/JDK 8 or 11, Hadoop, Spark 3.5.x, Hive, Streamlit.
- Repo layout: `parser/`, `mapreduce/`, `spark/`, `hive/`, `dashboard/`, `data/`, `docs/`.
- Create `parser/requirements.txt` listing all parser dependencies:
  ```
  html2text
  python-dateutil
  pyarrow
  pandas
  ```
- `.gitignore` for `data/` (privacy — do not commit emails).

### Phase 1 — Parser (`parser/parse_mbox.py`) (1–1.5 days)
1. Iterate `mailbox.mbox(...)`; handle the 1.3 GB in streaming fashion (no full-file `read()`).
2. Decode headers via `email.header.decode_header` (fix base64 subjects + folded labels).
3. Body extraction: prefer `text/plain`, fall back to `html2text` for HTML-only; skip attachments in text (count them separately).
4. **Dedup by `Message-ID` across the entire MBOX stream** — the same email can appear multiple times because Google Takeout emits one copy per Gmail label (e.g., Inbox + Starred = 2 entries). Maintain a `seen_ids` set during iteration and skip any `Message-ID` already processed.
5. Normalize dates via `email.utils.parsedate_to_datetime` → UTC.
6. Emit two outputs from a single parse pass:
   - **JSON Lines** (UTF-8) → `raw/structured/` on HDFS — used by Java MapReduce via `TextInputFormat` (no extra JARs required).
   - **Parquet** (via `pyarrow`) → `cleaned/` on HDFS — used by Spark for efficient columnar reads.
7. **Validate:** expect **≥ 7,100 unique records** after deduplication (raw MBOX count is 7,287; multi-label copies reduce this by roughly 1–5%); write a summary report; eyeball 10 records.

### Phase 2 — HDFS Ingestion (0.5 day)
- `hdfs dfs -mkdir -p` under `raw/mbox/ raw/structured/ cleaned/ features/ results/`.
- HDFS layout and naming convention:

  | HDFS path | Contents | Consumer |
  |---|---|---|
  | `raw/mbox/` | Original `.mbox` file | Archive / provenance |
  | `raw/structured/` | Parser-output **JSONL** (UTF-8, one record per line) | Java MapReduce (`TextInputFormat`) |
  | `cleaned/` | Parser-output **Parquet** (deduplicated, decoded, UTC-normalized) | PySpark |
  | `features/` | Spark-output Parquet (NLP-cleaned, classified, extracted) | Hive + Streamlit |
  | `results/` | MapReduce aggregate outputs | Docs / reporting |

- Naming rationale: `cleaned/` = structurally clean (decoded, deduped, normalized); `features/` = semantically enriched (NLP preprocessing + ML labels). Spark reads `cleaned/`, does NLP preprocessing, and writes to `features/` — it does not re-clean `cleaned/`.

### Phase 3 — Java MapReduce (1–1.5 days)
- One meaningful job, e.g. **"emails per sender-domain per month"** (counts + aggregation).
- Input: `raw/structured/` JSONL (read as plain text, one JSON object per line — no Parquet JARs needed).
- Mapper: parse each JSON line, extract `sender_domain` + `date_utc`; emit key `sender_domain + "\t" + yyyy-MM`, value `1`.
- Reducer: sum values per key → output `(domain, year-month, count)`.
- Output to `results/domain_month_counts/` on HDFS.
- Justify in docs: demonstrates classic batch + course Java-MR requirement.

### Phase 4 — Spark Pipeline (`spark/`) (2–3 days)
1. Load `cleaned/` parquet.
2. **Preprocessing:** lowercase, strip quoted replies (`> ...`), remove URLs/signatures, tokenize, spell-light normalization.
3. **Feature extraction:** TF-IDF (Spark MLlib `HashingTF` + `IDF`).
4. **Labeling strategy (weak supervision):**
   - Bootstrap with Gmail `Category` labels (promotions → Newsletter, purchases/bills → Administrative, etc.).
   - `forums` is a noisy signal (covers mailing lists, GitHub notifications, digests — not just events). Only map `forums` → Workshop/Event when **combined** with a subject/body keyword match ("workshop", "seminar", "webinar", "event"); otherwise leave it for the ML classifier rather than hard-labeling it.
   - Add **domain + keyword rules** for opportunity classes:
     - `dare2compete.news` / "hackathon" → Hackathon/Competition
     - `naukri.com` / "job|opening|placement" → Job/Placement
     - `eduskillsfoundation.org` / "internship" → Internship
     - `classroom.google.com` / college domain → Academic
   - Hybrid: rules for high-confidence, ML for the rest.
5. **Classifier:** Naive Bayes / Logistic Regression on TF-IDF → `category`.
6. Persist `features/` parquet with predicted `category` + confidence.

### Phase 5 — Information Extraction (1–1.5 days)
- `deadline`/`event_date`: date regex + `dateutil` parsing; normalize; **leave null if ambiguous** (never guess).
- `organization`: sender-domain mapping + simple NER/heuristics; null when unknown.
- `campus_type`: rule/heuristic (sender domain is college vs external).
- Store as derived columns in `features/`.

### Phase 6 — Hive Analytics (`hive/`) (1 day)
- External tables over `features/` parquet.
- Queries: count by category, month-over-month volume, internship/hackathon/interview counts, on vs off campus, read vs unread, top senders/domains, upcoming deadlines.

### Phase 7 — Streamlit Dashboard (`dashboard/`) (1–1.5 days)
- **Data source:** Streamlit reads the small result Parquet/CSV files produced by the Hive queries directly (`pandas.read_parquet` / `read_csv`) — no live Thrift/HiveServer2 connection required. This keeps the dashboard simple and avoids a runtime dependency on a running Hive service during demos.
- Summary cards (totals + unread + opportunities).
- Charts: category distribution (bar), volume over time (line), on/off campus (pie), read/unread.
- **Opportunity table** (filterable): `Opportunity | Type | Organization | Campus | Deadline`.

### Phase 8 — Integration, Test, Docs (1–1.5 days)
- End-to-end run on real dataset; verify counts across stages are consistent.
- Document the 5 Vs mapping, incremental-processing demo (append a new MBOX chunk and re-run Spark incrementally).
- Prepare viva talking points (Section 10).

---

## 7. Key Risks & Mitigations

| Risk | Mitigation |
|---|---|
| 1.3 GB parse slow / memory | Streaming iteration; batching; avoid full-file reads |
| HTML-only bodies with no text | `html2text` fallback; log coverage % |
| Encoding crashes (cp1252) | Always decode with `errors='replace'`; write UTF-8 |
| Weak labels bias classifier | Hybrid rules+ML; log precision on a hand-checked sample |
| "Not big enough" criticism | Emphasize growth trajectory + horizontal-scalable design + partitioning demo |
| Privacy (real emails) | Do not commit `data/`; strip personal identifiers in docs |

---

## 8. Team Division (suggested, 4 members)

| Member | Ownership |
|---|---|
| A | Phase 1–2: parser + HDFS ingestion |
| B | Phase 3: Java MapReduce |
| C | Phase 4–5: Spark + classification + extraction |
| D | Phase 6–8: Hive + dashboard + integration |

Overlap encouraged; parser (A) and Spark (C) are the critical paths.

---

## 9. Milestones & Effort (total ≈ 6–9 team-days)

1. Parser produces validated structured data — end of Phase 1.
2. Data visible in HDFS + one MapReduce job runs — Phase 3.
3. Classifier outputs categories on real data — Phase 4.
4. Opportunity extraction + Hive analytics + dashboard — Phases 5–7.
5. End-to-end demo + 5-Vs documentation — Phase 8.

---

## 10. Viva-Defensibility Cheatsheet

- **Why HDFS?** Distributed storage; data grows per-student/year; replicates blocks.
- **Why MapReduce?** Classic distributed batch; meets Java course requirement.
- **Why Spark?** In-memory processing for TF-IDF/classification over heterogeneous text.
- **Why Hive?** SQL analytics over processed data without custom code.
- **Why Hive instead of just Spark SQL?** Hive provides a persistent metastore and schema registry over the `features/` Parquet, so the analytics layer is queryable by any HiveServer2-compatible tool (Beeline, BI clients) independently of a running Spark job. Spark SQL would work too, but Hive keeps the analytics layer decoupled and reusable — and demonstrates the classic Hadoop-ecosystem query layer expected in a BDA course.
- **Why not Kafka/MongoDB/DL?** Velocity handled by incremental batch; DL/LLM unnecessary for explainable classification; MongoDB adds no needed capability.
- **5 Vs mapping:** grounded in Section 1 numbers (growth, unread/read, attachments, missing subjects, opportunity domains).

---

## 11. Implementation Status & Delivery Summary

**Status:** **ALL PHASES 1–7 COMPLETE & VALIDATED ✅**

1. **Phase 1 (Parser):** Completed (`parser/parse_mbox.py`). 7,276 unique records extracted and deduplicated.
2. **Phase 2 (HDFS Ingestion):** Completed. Structured JSONL & cleaned Parquet uploaded to HDFS.
3. **Phase 3 (Hadoop MapReduce):** Completed (`mapreduce/`). Java MR aggregated 1,113 `domain × month` keys across 7,276 records on YARN.
4. **Phase 4 (Spark MLlib Pipeline):** Completed (`spark/`). TF-IDF + Multinomial Logistic Regression achieved **90.49% accuracy** / **90.25% F1**.
5. **Phase 5 (Opportunity Extraction):** Completed (`spark/extract.py`). Extracted `campus_type`, `organization`, `opportunity_type`, 52 `deadlines`, and `keywords`.
6. **Phase 6 (Hive Analytics):** Completed (`hive/`). External table over HDFS Parquet queried for 7 analytical metrics; results materialized to CSV & Parquet.
7. **Phase 7 (Streamlit Dashboard):** Completed (`dashboard/`). Interactive web dashboard with executive KPI cards, 5 multi-metric tabs, filterable opportunity table, and ML inspector.
8. **Phase 8 (Integration & Docs):** Completed. Full viva cheatsheet, READMEs across all modules, and end-to-end reproducible runner scripts in place.