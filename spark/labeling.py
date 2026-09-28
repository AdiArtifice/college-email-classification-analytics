"""
Weak supervision labeling strategy for the email classification pipeline.

Implements:
1. High-confidence opportunity domain + keyword rules:
   - dare2compete.news / unstop -> Hackathon/Competition
   - naukri.com / job/placement keywords -> Job/Placement
   - eduskillsfoundation.org / internship keywords -> Internship
   - interview keywords -> Interview
   - scholarship keywords -> Scholarship
2. Institutional & Academic rules:
   - classroom.google.com / sjcem.edu.in -> Academic, Examination, or Administrative
3. Noisy signal handling:
   - Category forums is only mapped to Workshop/Event when combined with an explicit
     workshop/seminar/webinar/event keyword match. Otherwise left for ML.
4. Gmail Category bootstrap:
   - Category promotions -> Newsletter/Promotional
   - Category purchases / bills -> Administrative
   - Category social -> Miscellaneous
5. Hybrid boundary:
   - Unassigned rows return None, to be predicted by the Spark MLlib classifier.
"""

import re
from typing import List, Optional
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Precompiled regex patterns
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
    r'\b(internship|virtual internship|summer intern|winter intern|research intern|intern program)\b',
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

# Known job and opportunity portal domains
JOB_DOMAINS = {
    'naukri.com', 'foundit.in', 'linkedin.com', 'hirist.com',
    'apna.co', 'firstjob.in', 'monster.com', 'shine.com'
}

INTERN_DOMAINS = {
    'eduskillsfoundation.org', 'internshala.com', 'letsintern.com'
}

HACKATHON_DOMAINS = {
    'dare2compete.news', 'unstop.com', 'hackerearth.com', 'devfolio.co', 'kaggle.com'
}


def assign_weak_label(
    sender_domain: Optional[str],
    subject: Optional[str],
    body_text: Optional[str],
    gmail_labels: Optional[List[str]]
) -> Optional[str]:
    """
    Assign a weak supervision label based on deterministic domain and keyword rules,
    or bootstrap from Gmail category labels.

    Returns:
        A string category name if a rule matches with high confidence, or None if
        the record should be left for the ML classifier.
    """
    domain = str(sender_domain or '').lower().strip()
    subj = str(subject or '').lower().strip()
    body = str(body_text or '').lower().strip()
    combined_text = f'{subj} {body}'

    labels_set = set(gmail_labels) if gmail_labels is not None else set()

    # -------------------------------------------------------------
    # Priority 1: High-Confidence Opportunity Classes
    # -------------------------------------------------------------

    # 1a. Interview
    if 'interview' in subj or RE_INTERVIEW.search(combined_text):
        return 'Interview'

    # 1b. Scholarship
    if any(k in subj for k in ['scholarship', 'fellowship', 'financial aid']) or RE_SCHOLARSHIP.search(subj):
        return 'Scholarship'

    # 1c. Hackathon / Competition
    if domain in HACKATHON_DOMAINS or 'unstop' in domain:
        return 'Hackathon/Competition'
    if RE_HACKATHON.search(subj):
        return 'Hackathon/Competition'

    # 1d. Job / Placement vs Internship from career portals or subjects
    if domain in INTERN_DOMAINS or RE_INTERNSHIP.search(subj):
        return 'Internship'

    if domain in JOB_DOMAINS or 'placement' in domain or 'tpo' in domain or RE_JOB.search(subj):
        if RE_INTERNSHIP.search(combined_text) and not ('job' in subj or 'full-time' in combined_text):
            return 'Internship'
        return 'Job/Placement'

    # 1e. Workshop / Event from explicit subject keywords
    if RE_WORKSHOP_EVENT.search(subj):
        return 'Workshop/Event'

    # -------------------------------------------------------------
    # Priority 2: College & Academic Domain Rules
    # -------------------------------------------------------------
    if domain == 'classroom.google.com' or domain.endswith('sjcem.edu.in'):
        if RE_EXAMINATION.search(combined_text):
            return 'Examination'
        if RE_ADMINISTRATIVE.search(combined_text):
            return 'Administrative'
        return 'Academic'

    # -------------------------------------------------------------
    # Priority 3: Noisy Forum Label Handling
    # "forums is a noisy signal (covers mailing lists, GitHub notifications,
    # digests — not just events). Only map forums -> Workshop/Event when
    # combined with a subject/body keyword match; otherwise leave it for ML."
    # -------------------------------------------------------------
    if 'Category forums' in labels_set:
        if RE_WORKSHOP_EVENT.search(combined_text):
            return 'Workshop/Event'
        # Do not hard-label non-event forums emails; leave for ML

    # -------------------------------------------------------------
    # Priority 4: Bootstrap from Gmail Category Labels
    # -------------------------------------------------------------
    if 'Category purchases' in labels_set or 'Category bills' in labels_set:
        return 'Administrative'

    if 'Category promotions' in labels_set:
        return 'Newsletter/Promotional'

    if 'Category social' in labels_set:
        return 'Miscellaneous'

    # -------------------------------------------------------------
    # Priority 5: Unlabeled / Leave for ML
    # -------------------------------------------------------------
    return None


def get_weak_label_udf():
    """Return PySpark UDF for weak supervision labeling."""
    return udf(assign_weak_label, StringType())


# All supported target categories
TARGET_CATEGORIES = [
    'Academic',
    'Administrative',
    'Examination',
    'Hackathon/Competition',
    'Internship',
    'Interview',
    'Job/Placement',
    'Miscellaneous',
    'Newsletter/Promotional',
    'Scholarship',
    'Workshop/Event'
]
