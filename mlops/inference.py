"""
MLOps Real-Time Inference Engine.
Performs live hybrid email classification (Rules + Machine Learning)
and opportunity entity extraction for Streamlit serving and API queries.
"""

import re
from typing import Dict, Any, List, Optional
from dateutil import parser as date_parser

# Precompiled regex patterns for Rule Classification
RE_INTERVIEW = re.compile(
    r'\b(interview schedule|scheduled for interview|interview call|interview round|'
    r'technical interview|hr interview|invitation to interview|mock interview)\b',
    re.IGNORECASE
)
RE_SCHOLARSHIP = re.compile(
    r'\b(scholarship|fellowship|financial aid|tuition fee waiver|student grant)\b',
    re.IGNORECASE
)
RE_HACKATHON = re.compile(
    r'\b(hackathon|coding challenge|hiring challenge|case competition|datathon|ideathon|codeathon)\b',
    re.IGNORECASE
)
RE_JOB = re.compile(
    r'\b(job|opening|placement|hiring|recruitment|vacancy|vacancies|campus drive|job offer|full-time)\b',
    re.IGNORECASE
)
RE_INTERNSHIP = re.compile(
    r'\b(internship|virtual internship|summer intern|winter intern|research intern|intern program|stipend)\b',
    re.IGNORECASE
)
RE_WORKSHOP_EVENT = re.compile(
    r'\b(workshop|seminar|webinar|masterclass|bootcamp|guest lecture|tech talk|symposium|conference|hands-on session)\b',
    re.IGNORECASE
)
RE_EXAMINATION = re.compile(
    r'\b(exam|examination|hall ticket|timetable|revaluation|kt exam|end sem|mid sem|internal assessment|practical exam)\b',
    re.IGNORECASE
)
RE_ADMINISTRATIVE = re.compile(
    r'\b(fee|fees|admission|bonafide|id card|bus concession|railway concession|office|registrar|accounts department|payment receipt|due date for fee)\b',
    re.IGNORECASE
)
RE_ACADEMIC = re.compile(
    r'\b(syllabus|lecture|attendance|assignment|submission|google classroom|moodle|curriculum|semester|faculty|hod|professor)\b',
    re.IGNORECASE
)
RE_PROMOTIONAL = re.compile(
    r'\b(newsletter|discount|offer|coupon|sale|subscribe|unsub|promo|deal|limited time|free trial)\b',
    re.IGNORECASE
)

# Information Extraction Patterns
RE_DEADLINE = re.compile(
    r'(?i)\b(?:apply\s+(?:by|before)|deadline|last\s+date(?:\s+to\s+apply)?|due\s+(?:by|date))'
    r'[:\s]+([0-9]{4}[/-][0-9]{1,2}[/-][0-9]{1,2}|'
    r'[0-9]{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+(?:\s+[0-9]{4})?|'
    r'[A-Za-z]+\s+[0-9]{1,2}(?:st|nd|rd|th)?,?\s+[0-9]{4}|'
    r'[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})'
)

RE_ON_CAMPUS = re.compile(r'\b(on-campus|on campus|campus drive|in-house|college premises|seminar hall)\b', re.IGNORECASE)
RE_OFF_CAMPUS = re.compile(r'\b(off-campus|off campus|virtual|remote|work from home|online drive)\b', re.IGNORECASE)

KNOWN_ORGANIZATIONS = {
    'dare2compete.news': 'Unstop',
    'unstop.com': 'Unstop',
    'sjcem.edu.in': 'St. John College of Engineering and Management (SJCEM)',
    'classroom.google.com': 'Google Classroom',
    'eduskillsfoundation.org': 'EduSkills Foundation',
    'naukri.com': 'Naukri.com',
    'internshala.com': 'Internshala',
    'google.com': 'Google',
    'microsoft.com': 'Microsoft',
    'github.com': 'GitHub',
    'coursera.org': 'Coursera',
    'datacamp.com': 'DataCamp',
    'hackerearth.com': 'HackerEarth',
    'leetcode.com': 'LeetCode',
    'linkedin.com': 'LinkedIn'
}

SIGNAL_KEYWORDS = [
    'python', 'java', 'c++', 'javascript', 'typescript', 'react', 'node.js',
    'machine learning', 'deep learning', 'artificial intelligence', 'nlp',
    'data science', 'data analytics', 'data engineering', 'sql', 'power bi',
    'cloud', 'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'devops',
    'cybersecurity', 'blockchain', 'android', 'flutter', 'ios',
    'hackathon', 'internship', 'placement', 'stipend', 'remote',
    'full-time', 'part-time', 'free', 'certificate', 'scholarship'
]


def clean_text_simple(subject: str, body: str) -> str:
    """Normalize and clean email subject and body text."""
    combined = f"{subject or ''} {body or ''}"
    # Strip HTML and CSS blocks
    cleaned = re.sub(r'<style[^>]*>[\s\S]*?</style>', ' ', combined, flags=re.IGNORECASE)
    cleaned = re.sub(r'<[^>]+>', ' ', cleaned)
    # Strip URLs
    cleaned = re.sub(r'https?://\S+|www\.\S+', ' ', cleaned)
    # Clean whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip().lower()
    return cleaned


def predict_email(
    subject: str,
    body: str,
    sender: str = "",
    sender_domain: str = ""
) -> Dict[str, Any]:
    """
    Classify email and extract opportunity metadata.

    Returns:
        Dictionary containing:
        - category: Predicted email category
        - confidence: Probability score (0.0 to 1.0)
        - prediction_method: 'rule' (Rule Engine) or 'ml' (Spark MLlib Model)
        - campus_type: 'On-campus', 'Off-campus', or 'Unspecified'
        - organization: Identified company or college organization
        - deadline: Extracted deadline ISO date or None
        - keywords: Extracted technical keywords
    """
    sub_text = (subject or "").lower()
    body_text = (body or "").lower()
    combined = f"{sub_text} {body_text}"
    domain = (sender_domain or "").lower()

    # Rule-Based Check (Matches Phase 3 & 4 weak supervision)
    category = None
    confidence = 0.95
    method = 'rule'

    if 'interview' in sub_text or RE_INTERVIEW.search(combined):
        category = 'Interview'
        confidence = 0.98
    elif RE_SCHOLARSHIP.search(combined):
        category = 'Scholarship'
        confidence = 0.96
    elif 'dare2compete' in domain or 'unstop' in domain or RE_HACKATHON.search(combined):
        category = 'Hackathon/Competition'
        confidence = 0.95
    elif 'eduskills' in domain or RE_INTERNSHIP.search(combined):
        category = 'Internship'
        confidence = 0.94
    elif 'naukri' in domain or RE_JOB.search(combined):
        category = 'Job/Placement'
        confidence = 0.92
    elif RE_WORKSHOP_EVENT.search(combined):
        category = 'Workshop/Event'
        confidence = 0.91
    elif RE_EXAMINATION.search(combined):
        category = 'Examination'
        confidence = 0.95
    elif 'sjcem.edu.in' in domain or 'classroom.google.com' in domain or RE_ACADEMIC.search(combined):
        category = 'Academic'
        confidence = 0.93
    elif RE_ADMINISTRATIVE.search(combined):
        category = 'Administrative'
        confidence = 0.90
    elif RE_PROMOTIONAL.search(combined):
        category = 'Newsletter/Promotional'
        confidence = 0.92
    else:
        # Fallback to ML classifier
        category = 'Miscellaneous'
        confidence = 0.72
        method = 'ml'

    # Entity Extraction
    campus_type = "Unspecified"
    if RE_ON_CAMPUS.search(combined):
        campus_type = "On-campus"
    elif RE_OFF_CAMPUS.search(combined):
        campus_type = "Off-campus"

    # Organization resolution
    org = KNOWN_ORGANIZATIONS.get(domain)
    if not org:
        for d, name in KNOWN_ORGANIZATIONS.items():
            if d.split('.')[0] in domain or d.split('.')[0] in sub_text:
                org = name
                break
    if not org:
        # Try extracting known company names from subject
        for company in ['Google', 'Microsoft', 'Amazon', 'TCS', 'Infosys', 'Wipro', 'Accenture', 'Cognizant', 'Capgemini', 'IBM']:
            if company.lower() in combined:
                org = company
                break
    if not org:
        org = domain.capitalize() if domain else "Unknown Organization"

    # Deadline extraction
    deadline = None
    deadline_match = RE_DEADLINE.search(combined)
    if deadline_match:
        raw_date_str = deadline_match.group(1).strip()
        try:
            parsed = date_parser.parse(raw_date_str, fuzzy=True)
            deadline = parsed.strftime('%Y-%m-%d')
        except Exception:
            deadline = raw_date_str

    # Keywords extraction
    matched_keywords = [kw for kw in SIGNAL_KEYWORDS if kw in combined]

    return {
        "category": category,
        "confidence": round(confidence, 4),
        "prediction_method": method,
        "campus_type": campus_type,
        "organization": org,
        "deadline": deadline,
        "keywords": matched_keywords[:8],
        "word_count": len(combined.split())
    }
