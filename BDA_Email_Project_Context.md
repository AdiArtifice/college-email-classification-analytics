# Intelligent College Email Classification and Opportunity Analytics System

## 1. Project Overview

This is a **Big Data Analytics (BDA) mini-project** designed for a team of college students.

The system addresses the problem of **email overload among college students**. Institutional/college Gmail accounts can accumulate thousands of emails containing internships, placements, interviews, hackathons, competitions, workshops, academic notices, administrative communication, newsletters, and other messages. Important opportunities can easily be buried among routine or irrelevant emails.

The proposed system will process a student's email dataset, classify emails into meaningful categories, extract useful opportunity-related information, perform Big Data analytics, and present the results through a personalized dashboard.

### Final project title

**Intelligent College Email Classification and Opportunity Analytics System**

---

## 2. Intended Users vs Prototype Dataset

The **intended system is general-purpose for college students**, not a personal email-cleaning tool.

Each student should conceptually use their own email data and receive their own personalized analytics.

However, the **prototype will use one real dataset: approximately 6,000 emails from the developer's college Gmail account**, including roughly 3,000 unread emails.

This dataset is being used as the initial development and demonstration dataset. It should not be assumed that every student has the same number of emails.

The eventual architecture may support authorized Gmail access for individual users, but a full multi-user production application is **outside the mini-project scope**.

---

## 3. Data Acquisition

The initial dataset is expected to be obtained using **Google Takeout**, which can export Gmail message content, headers, attachments, and Gmail labels. Work/school Google Workspace accounts may have administrator-controlled export restrictions.

The Gmail export is expected to contain an **MBOX-based mailbox archive**.

The raw MBOX should be retained as the source dataset, while a parser will convert individual messages into a structured representation suitable for Big Data processing.

Possible base fields:

- email_id
- sender
- recipient
- sender_domain
- subject
- date/time
- body
- labels
- read/unread status where available
- attachment indicator/metadata

Generated fields may include:

- category
- opportunity_type
- campus_type
- organization
- deadline
- event_date
- location
- keywords

The exact schema will be finalized after inspecting the real export.

---

## 4. Core BDA Objective

The project must be a genuine **Big Data Analytics project**, rather than simply an NLP classifier with a dashboard.

The intended pipeline is:

**Data Ingestion → Distributed Storage → Preprocessing → Distributed Processing → Classification/Information Extraction → Analytics → Visualization**

The project should demonstrate the **5 Vs of Big Data** naturally:

### Volume
Thousands of accumulated emails per student, with the ability to demonstrate larger/scaled datasets if necessary.

### Velocity
New emails continuously arrive. The prototype should support or demonstrate periodic/incremental processing of newly received emails rather than requiring full real-time streaming.

### Variety
Email data contains different message types, senders, subjects, text/HTML content, labels, attachments/metadata, and heterogeneous communication.

### Veracity
The dataset can contain duplicates, spam/noise, irrelevant emails, missing information, inconsistent formatting, and ambiguous dates/deadlines. Preprocessing should address these issues.

### Value
The system converts email overload into actionable information: opportunities, deadlines, categories, trends, and summarized analytics.

---

## 5. Proposed Technology Stack

### Gmail / Google Takeout
Initial data source and development dataset.

### Gmail API
Potential ingestion mechanism for the eventual user-facing version, allowing an authorized student to connect their own Gmail account. It is not necessary to build a complete production OAuth application for the prototype.

### HDFS
Distributed storage for raw and processed datasets.

Suggested conceptual structure:

```text
HDFS
├── raw/
├── cleaned/
├── features/
└── results/
```

### Hadoop MapReduce (Java)
Used for at least one meaningful distributed batch-processing task. A suitable example is calculating category frequencies or other aggregate statistics.

This also aligns with the BDA coursework requirement to demonstrate Java MapReduce.

### Apache Spark / PySpark
Primary processing engine for:

- Data transformation
- Cleaning
- Deduplication
- Text preprocessing
- Feature extraction
- Classification
- Aggregation
- Large-scale analytics

### Spark MLlib
Potentially used for traditional email classification. A feasible initial approach is:

**Subject + Body → Text preprocessing → TF-IDF → Naive Bayes / Logistic Regression → Category**

The project should avoid unnecessary deep learning or LLM dependence.

### Apache Hive
Analytical/query layer over the processed data.

Potential analytics include:

- Email count by category
- Monthly/temporal trends
- Internship/hackathon/interview counts
- On-campus vs off-campus distribution
- Frequent senders/domains
- Read vs unread distribution
- Upcoming deadlines

### Streamlit
Lightweight dashboard for displaying the processed analytics and opportunity information.

---

## 6. Technologies Currently Excluded

The project should not use technologies merely to increase the technology count.

### MongoDB — currently excluded
HDFS + Spark + Hive already provide the required storage, processing, and analytical layers. MongoDB does not currently solve a necessary problem.

### Kafka — currently excluded
Although emails have velocity, a Gmail → Kafka → Spark Streaming pipeline would add substantial complexity. Periodic/incremental processing is sufficient for the prototype.

### Also not currently required
- HBase
- Elasticsearch
- Airflow
- Deep-learning models
- LLM APIs
- Complex frontend frameworks

These may only be reconsidered if a concrete requirement emerges.

---

## 7. Proposed Processing Architecture

```text
                    COLLEGE GMAIL
                         |
                  Google Takeout
                         |
                        MBOX
                         |
                  Email Parser
                         |
                Structured Email Data
                         |
                        HDFS
                         |
             +-----------+-----------+
             |                       |
             v                       v
      Hadoop MapReduce         Apache Spark
          (Java)               (PySpark)
             |                       |
      Batch Analytics        +-------+--------+
                             |                |
                          NLP/ML       Information
                       Classification   Extraction
                             |                |
                             +-------+--------+
                                     |
                              Processed Data
                                     |
                                    Hive
                                     |
                              SQL Analytics
                                     |
                                     v
                                Streamlit
                                 Dashboard
```

Hadoop MapReduce and Spark do not need to perform identical work. MapReduce should demonstrate a meaningful distributed batch task, while Spark should handle the more extensive processing/NLP/analytics workload.

---

## 8. Classification Scope

The exact categories will be refined after inspecting the dataset, but the current candidate categories are:

### Opportunities
- Internship
- Job / Placement
- Interview
- Hackathon
- Competition
- Workshop / Seminar / Event
- Scholarship

### College / Academic
- Academic
- Examination
- Assignment/course communication
- Administrative

### Other
- Newsletter / Promotional
- Miscellaneous

**On-campus vs Off-campus** should preferably be an additional attribute rather than separate categories.

Example:

```text
Category: Internship
Campus Type: Off-campus
```

or

```text
Category: Interview
Campus Type: On-campus
```

---

## 9. Information Extraction

The system should go beyond classification for opportunity-related emails.

Possible extracted information:

- Organization
- Opportunity type
- On-campus / off-campus
- Deadline
- Event/application date
- Location
- Relevant keywords

Example:

```text
Input:
"ABC Technologies is offering an AI/ML internship.
Apply before 25 September 2026."

Output:
Category: Internship
Organization: ABC Technologies
Campus Type: Off-campus
Domain: AI/ML
Deadline: 25-09-2026
```

If a deadline or other field cannot be reliably determined, it should remain unknown/null rather than being guessed.

---

## 10. Dashboard Scope

The dashboard should answer two main questions:

1. **What is happening in the student's email inbox?**
2. **What important opportunities might the student need to know about?**

Possible dashboard elements:

### Summary
- Total emails
- Unread emails
- Opportunity emails
- Internships
- Hackathons
- Interviews
- Upcoming deadlines

### Analytics
- Emails by category
- Email volume over time
- Opportunity trends
- On-campus vs off-campus
- Read vs unread
- Sender/domain analysis

### Opportunity view
A filterable table containing fields such as:

```text
Opportunity | Type | Organization | Campus | Deadline
```

The dashboard is a presentation layer; the main BDA processing should happen before the dashboard.

---

## 11. Prototype Scope Boundaries

### IN SCOPE

- Approximately 6,000 real college emails as the initial dataset
- MBOX parsing
- Data cleaning and preprocessing
- HDFS storage
- Hadoop MapReduce task
- Spark-based processing
- Email classification
- Basic information extraction
- Hive analytics
- Streamlit dashboard
- Demonstration of the 5 Vs
- Demonstration of incremental/new-email processing if feasible

### OUT OF SCOPE

- Full production SaaS platform
- Multi-user deployment
- Permanent centralized storage of everyone's private emails
- Email sending/replying/forwarding
- Mobile application
- Full Gmail replacement
- Kafka-based streaming infrastructure
- Deep-learning/LLM-based understanding
- Perfect semantic interpretation of every email

---

## 12. Development Plan

1. Export and inspect the Gmail dataset.
2. Understand the MBOX structure.
3. Build the email parser.
4. Finalize the structured schema based on real data.
5. Store raw/processed data in HDFS.
6. Implement preprocessing and deduplication.
7. Implement a meaningful Java Hadoop MapReduce task.
8. Implement Spark processing.
9. Develop and evaluate the email classifier.
10. Add opportunity information extraction.
11. Create Hive tables and analytical queries.
12. Build the Streamlit dashboard.
13. Test with the real dataset.
14. Demonstrate scalability, the 5 Vs, and incremental processing where practical.

---

## 13. Key Design Principle

The project should remain **BDA-first**.

NLP/ML is a supporting component for classification and extraction. The dashboard is a visualization component. The central focus is the processing and analytics of a large, heterogeneous and continuously growing email dataset using distributed Big Data technologies.

Every technology should have a clear purpose:

```text
Gmail/Takeout → Data source
HDFS          → Distributed storage
MapReduce     → Distributed batch processing
Spark         → Main processing + NLP/ML + aggregation
Hive          → Analytical querying
Streamlit     → Visualization
```

Do not add technologies simply to make the architecture look larger.

---

## 14. Current Decisions vs Open Decisions

### Already decided
- Project title
- General student-focused use case
- Prototype uses approximately 6,000 college emails
- BDA/5-V focus
- HDFS + Hadoop MapReduce + Spark + Hive + Streamlit as the core stack
- MongoDB and Kafka are currently excluded
- Personalized dashboard concept
- Opportunity classification and information extraction are core features

### Finalized Implementation Decisions (All Complete ✅)
- **Final Schema:** 17 base attributes in `cleaned/` Parquet and 27 attributes in `features/` Parquet (including `category`, `confidence`, `prediction_method`, `campus_type`, `organization`, `opportunity_type`, `deadline`, `event_date`, `keywords`).
- **Category Hierarchy:** 11 mutually exclusive classes (Newsletter/Promotional, Academic, Hackathon/Competition, Internship, Interview, Job/Placement, Miscellaneous, Workshop/Event, Administrative, Examination, Scholarship).
- **Classification Strategy:** Hybrid weak supervision (72.3% high-confidence rule/bootstrap seeds) + Spark MLlib Multinomial Logistic Regression on TF-IDF (27.7% inference; 90.49% accuracy).
- **Extraction Method:** Conservative regex + `python-dateutil` normalized to ISO `YYYY-MM-DD` (never guessing when absent; 52 verified deadlines cataloged).
- **Storage & Query Layer:** Snappy Parquet in HDFS (`/user/student/email_project/`) queried via Apache Hive external tables, materialized to CSV/Parquet for Streamlit.

---

## 15. Guidance for Future Work

When reasoning about this project:

- Be critical rather than automatically agreeing with proposed ideas.
- Prioritize feasibility for a college BDA mini-project.
- Ensure the system has genuine Big Data justification.
- Avoid unnecessary technologies.
- Preserve the distinction between the **prototype dataset** and the **general-purpose system design**.
- Do not assume all students have the same email volume.
- Treat privacy as an important consideration because the prototype uses real email data.
- Prefer simple, explainable NLP/ML methods where they are sufficient.
- Make every technology in the architecture defensible in a viva.
- Keep the project focused on **Big Data processing and analytics**, not on building a production Gmail application.

## Current One-Line Definition

**A personalized Big Data email analytics pipeline that processes a student's large college-email collection, classifies communications, extracts actionable opportunity information, and presents analytical insights through a dashboard.**
