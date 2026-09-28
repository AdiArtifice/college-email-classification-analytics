"""
Information extraction for opportunity emails.

Extracts:
- campus_type: 'On-campus', 'Off-campus', or None
- organization: clean organization name (Unstop, Naukri, SJCEM, EduSkills, etc.)
- opportunity_type: fine-grained sub-domain ('AI/ML', 'Web Development', 'Data Science', etc.)
- deadline: ISO-8601 date (YYYY-MM-DD) or None (never guessed)
- event_date: ISO-8601 date (YYYY-MM-DD) or None
- keywords: list of high-signal skills/technologies/terms found in the email
"""

import re
from typing import List, Optional
from dateutil import parser as date_parser
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, ArrayType

# Domain -> Organization mapping
ORGANIZATION_MAP = {
    'dare2compete.news': 'Unstop',
    'unstop.com': 'Unstop',
    'sjcem.edu.in': 'SJCEM',
    'classroom.google.com': 'Google Classroom',
    'eduskillsfoundation.org': 'EduSkills',
    'naukri.com': 'Naukri',
    'internshala.com': 'Internshala',
    'mail.internshala.com': 'Internshala',
    'educative.io': 'Educative',
    'coursera.org': 'Coursera',
    'm.learn.coursera.org': 'Coursera',
    'datacamp.com': 'DataCamp',
    'mail.datacamp.com': 'DataCamp',
    'email.microsoft.com': 'Microsoft',
    'microsoft.com': 'Microsoft',
    'github.com': 'GitHub',
    'roboflow.com': 'Roboflow',
    'codedex.io': 'Codédex',
    'geeksforgeeks.org': 'GeeksforGeeks',
    'hackerearth.com': 'HackerEarth',
    'leetcode.com': 'LeetCode',
    'foundit.in': 'Foundit',
    'linkedin.com': 'LinkedIn',
    'apna.co': 'Apna',
    'udemy.com': 'Udemy',
    'students.udemy.com': 'Udemy',
    'accounts.google.com': 'Google',
    'google.com': 'Google',
    'mail.instagram.com': 'Instagram'
}

OPPORTUNITY_CATEGORIES = {
    'Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event'
}

# Regex for deadline extraction
RE_DEADLINE = re.compile(
    r'(?i)\b(?:apply\s+(?:by|before)|deadline|last\s+date(?:\s+to\s+apply)?|due\s+(?:by|date))'
    r'[:\s]+([0-9]{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+(?:\s+[0-9]{4})?|'
    r'[A-Za-z]+\s+[0-9]{1,2}(?:st|nd|rd|th)?,?\s+[0-9]{4}|'
    r'[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})'
)

# Regex for event date extraction
RE_EVENT_DATE = re.compile(
    r'(?i)\b(?:event\s+date|date\s+of\s+event|webinar\s+on|scheduled\s+on|session\s+on|held\s+on)'
    r'[:\s]+([0-9]{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+(?:\s+[0-9]{4})?|'
    r'[A-Za-z]+\s+[0-9]{1,2}(?:st|nd|rd|th)?,?\s+[0-9]{4}|'
    r'[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})'
)

# Keywords to match and extract
SIGNAL_KEYWORDS = [
    'python', 'java', 'c++', 'javascript', 'typescript', 'react', 'node.js',
    'machine learning', 'deep learning', 'artificial intelligence', 'nlp',
    'data science', 'data analytics', 'data engineering', 'sql', 'power bi',
    'cloud', 'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'devops',
    'cybersecurity', 'blockchain', 'android', 'flutter', 'ios',
    'hackathon', 'internship', 'placement', 'stipend', 'remote',
    'full time', 'part time', 'certificate', 'free', 'scholarship'
]


def extract_campus_type(domain: Optional[str], category: Optional[str]) -> Optional[str]:
    """Determine whether an opportunity is On-campus or Off-campus."""
    if not category or category not in OPPORTUNITY_CATEGORIES and category not in ('Academic', 'Examination'):
        return None
    d = str(domain or '').lower()
    if 'sjcem.edu.in' in d:
        return 'On-campus'
    return 'Off-campus'


def extract_organization(domain: Optional[str], sender: Optional[str]) -> Optional[str]:
    """Extract clean organization name from sender domain or sender header."""
    d = str(domain or '').lower().strip()
    if d in ORGANIZATION_MAP:
        return ORGANIZATION_MAP[d]
    for key_domain, org_name in ORGANIZATION_MAP.items():
        if d.endswith(key_domain):
            return org_name
    s = str(sender or '').strip()
    if s and not s.startswith('<') and len(s) < 40 and not '@' in s:
        clean_sender = re.sub(r'\(.*?\)', '', s).strip()
        if len(clean_sender) > 2:
            return clean_sender
    return None


def extract_opportunity_type(category: Optional[str], subject: Optional[str], body_text: Optional[str]) -> Optional[str]:
    """Identify fine-grained domain for opportunity emails (AI/ML, Data Science, etc.)."""
    if not category or category not in OPPORTUNITY_CATEGORIES:
        return None
    text = f'{subject or ""} {body_text or ""}'.lower()
    if re.search(r'\b(ai|artificial intelligence|machine learning|deep learning|nlp|llm|genai|computer vision)\b', text):
        return 'AI/ML'
    if re.search(r'\b(data science|data analyst|data analytics|data engineering|big data)\b', text):
        return 'Data Science'
    if re.search(r'\b(web development|full stack|frontend|backend|react|node|javascript|html/css)\b', text):
        return 'Web Development'
    if re.search(r'\b(cloud|aws|azure|gcp|devops|docker|kubernetes)\b', text):
        return 'Cloud & DevOps'
    if re.search(r'\b(cybersecurity|security|ethical hacking|infosec)\b', text):
        return 'Cybersecurity'
    if re.search(r'\b(software engineer|software developer|sde|coding|java|python|c\+\+)\b', text):
        return 'Software Engineering'
    return 'General Tech'


def extract_deadline(subject: Optional[str], body_text: Optional[str]) -> Optional[str]:
    """Extract explicit deadline if present; returns YYYY-MM-DD or None (never guesses)."""
    text = f'{subject or ""} {body_text or ""}'
    m = RE_DEADLINE.search(text)
    if m:
        raw_date = m.group(1).strip()
        try:
            parsed = date_parser.parse(raw_date, fuzzy=True, default=date_parser.parse('2026-01-01'))
            if 2023 <= parsed.year <= 2027:
                return parsed.strftime('%Y-%m-%d')
        except Exception:
            pass
    return None


def extract_event_date(subject: Optional[str], body_text: Optional[str]) -> Optional[str]:
    """Extract event date if explicitly present; returns YYYY-MM-DD or None."""
    text = f'{subject or ""} {body_text or ""}'
    m = RE_EVENT_DATE.search(text)
    if m:
        raw_date = m.group(1).strip()
        try:
            parsed = date_parser.parse(raw_date, fuzzy=True, default=date_parser.parse('2026-01-01'))
            if 2023 <= parsed.year <= 2027:
                return parsed.strftime('%Y-%m-%d')
        except Exception:
            pass
    return None


def extract_keywords(subject: Optional[str], body_text: Optional[str]) -> List[str]:
    """Extract relevant keywords matching known technology and opportunity terms."""
    text = f'{subject or ""} {body_text or ""}'.lower()
    found = []
    for kw in SIGNAL_KEYWORDS:
        if re.search(r'\b' + re.escape(kw) + r'\b', text):
            found.append(kw)
    return found


# PySpark UDFs
udf_campus_type = udf(extract_campus_type, StringType())
udf_organization = udf(extract_organization, StringType())
udf_opportunity_type = udf(extract_opportunity_type, StringType())
udf_deadline = udf(extract_deadline, StringType())
udf_event_date = udf(extract_event_date, StringType())
udf_keywords = udf(extract_keywords, ArrayType(StringType()))


def enrich_opportunities(df: DataFrame) -> DataFrame:
    """Enrich DataFrame with opportunity metadata columns."""
    return df.withColumn(
        'campus_type',
        udf_campus_type(col('sender_domain'), col('category'))
    ).withColumn(
        'organization',
        udf_organization(col('sender_domain'), col('sender'))
    ).withColumn(
        'opportunity_type',
        udf_opportunity_type(col('category'), col('subject'), col('body_text'))
    ).withColumn(
        'deadline',
        udf_deadline(col('subject'), col('body_text'))
    ).withColumn(
        'event_date',
        udf_event_date(col('subject'), col('body_text'))
    ).withColumn(
        'keywords',
        udf_keywords(col('subject'), col('body_text'))
    )
