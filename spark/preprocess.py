"""
Text preprocessing and normalization for the Spark email classification pipeline.

Handles:
- Lowercasing
- Stripping CSS fragments and inline styles (common in html2text fallback output)
- Stripping quoted replies (> ..., On ... wrote:, Forwarded message headers)
- Removing URLs, email addresses, and signatures
- Spell-light normalization (character repetition reduction, non-alpha cleanup)
- Subject boosting (weighting subject tokens)
"""

import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Regex patterns precompiled for performance across Spark partitions
RE_CSS_RULES = re.compile(r'\b(body|table|td|th|div|span|p|a|ul|li|h[1-6]|img)\s*[,{][^}]*}', re.IGNORECASE)
RE_INLINE_STYLES = re.compile(r'\{[^\}]*\}')
RE_MEDIA_QUERIES = re.compile(r'@media[^{]*\{[^{}]*\{[^{}]*\}[^{}]*\}', re.IGNORECASE)

RE_QUOTED_LINES = re.compile(r'(?m)^\s*>.*$')
RE_ON_WROTE = re.compile(r'(?m)^On\s+.*?wrote:\s*$', re.IGNORECASE)
RE_FORWARDED = re.compile(r'-{3,}\s*Forwarded message\s*-{3,}', re.IGNORECASE)
RE_HEADER_BLOCKS = re.compile(r'(?m)^(From|To|Sent|Subject|Date):\s+.+$')

RE_URLS = re.compile(r'https?://\S+|www\.\S+')
RE_EMAILS = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')

RE_SIGNATURE_DELIM = re.compile(r'(?m)^--\s*$.*', re.DOTALL)
RE_SIGNATURE_TEXT = re.compile(
    r'(?m)^(Thanks & Regards|Best regards|Warm regards|With regards|Regards|Sincerely|Yours faithfully)[\s\S]*$',
    re.IGNORECASE
)

RE_MD_IMAGES = re.compile(r'\[image:[^\]]*\]', re.IGNORECASE)
RE_MD_LINKS = re.compile(r'\[([^\]]+)\]\([^\)]*\)')

RE_CHAR_REPEAT = re.compile(r'(.)\1{2,}')
RE_NON_ALPHA = re.compile(r'[^a-z\s]')
RE_WHITESPACE = re.compile(r'\s+')


def clean_text(subject: str, body: str) -> str:
    """
    Clean, normalize, and combine email subject and body for TF-IDF feature extraction.
    """
    subject_str = str(subject or '').strip()
    body_str = str(body or '').strip()

    # 1. Clean CSS noise from body (from html2text conversion)
    body_str = RE_MEDIA_QUERIES.sub(' ', body_str)
    body_str = RE_CSS_RULES.sub(' ', body_str)
    body_str = RE_INLINE_STYLES.sub(' ', body_str)

    # 2. Strip quoted replies and forwarded headers
    body_str = RE_QUOTED_LINES.sub(' ', body_str)
    body_str = RE_ON_WROTE.sub(' ', body_str)
    body_str = RE_FORWARDED.sub(' ', body_str)
    body_str = RE_HEADER_BLOCKS.sub(' ', body_str)

    # 3. Strip URLs and email addresses
    body_str = RE_URLS.sub(' ', body_str)
    subject_str = RE_URLS.sub(' ', subject_str)
    body_str = RE_EMAILS.sub(' ', body_str)

    # 4. Strip signatures and sign-offs
    body_str = RE_SIGNATURE_DELIM.sub(' ', body_str)
    body_str = RE_SIGNATURE_TEXT.sub(' ', body_str)

    # 5. Remove markdown image/link artifacts
    body_str = RE_MD_IMAGES.sub(' ', body_str)
    body_str = RE_MD_LINKS.sub(r'\1', body_str)

    # 6. Combine subject and body (give subject higher weight by repeating it)
    combined = f'{subject_str} {subject_str} {body_str}'.lower()

    # 7. Spell-light normalization:
    # Reduce elongated words: e.g. "freeeee" -> "free", "sooooo" -> "soo"
    combined = RE_CHAR_REPEAT.sub(r'\1\1', combined)

    # Keep only lowercase alphabetic characters and spaces
    combined = RE_NON_ALPHA.sub(' ', combined)

    # Collapse extra whitespace
    combined = RE_WHITESPACE.sub(' ', combined).strip()

    return combined


def get_clean_text_udf():
    """Return PySpark UDF for text cleaning."""
    return udf(clean_text, StringType())


if __name__ == '__main__':
    # Quick standalone test
    sample_sub = "URGENT: Hackathon 2026 Registration Open!!!"
    sample_body = """> Earlier message
Check out https://unstop.com/hackathon
--
Best regards,
Organizer Team"""
    cleaned = clean_text(sample_sub, sample_body)
    print("Test clean_text output:")
    print(cleaned)
