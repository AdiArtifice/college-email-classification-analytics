# Intelligent College Email Classification and Opportunity Analytics System

**Big Data Analytics (BDA) Capstone Project**  
*A distributed, end-to-end data engineering, NLP classification, SQL analytics, and visualization pipeline for college student inboxes.*

---

## 📌 Executive Summary

Institutional student email accounts accumulate thousands of messages containing internships, job placements, hackathons, seminars, course updates, and promotional spam. Important career opportunities are frequently buried under routine inbox clutter.

This project implements a **BDA-first, distributed analytics pipeline** that converts a 1.3 GB Gmail mailbox export into structured records, classifies communications via **Weak Supervision + Spark MLlib**, extracts actionable opportunity metadata (deadlines, organizations, domains), executes analytical queries in **Apache Hive**, and visualizes insights on an interactive **Streamlit dashboard**.

- The dashboard reads materialized analytics outputs from `data/output/analytics/`, so it can be demonstrated without a live Hadoop or Spark cluster.

- **Dataset:** 7,287 raw emails (~1.3 GB MBOX) → **7,276 unique records** deduplicated via RFC 822 `Message-ID`.
- **Classification Performance:** **90.49% Test Accuracy** and **90.25% Weighted F1** via Multinomial Logistic Regression on distributed TF-IDF features.
- **Serving Architecture:** Decoupled Lambda-style serving layer; queries materialized into Parquet/CSV for sub-second dashboard rendering without live cluster dependencies.

---

## 📊 Dataset Reality Check (As Measured)

| Attribute | Value | Significance |
|---|---|---|
| **Raw Mailbox Archive** | `All mail Including Spam and Trash.mbox` | Full Google Takeout export |
| **Raw Archive Size** | **1,299,726,336 bytes (~1.3 GB)** | Multi-year student inbox |
| **Raw Message Count** | **7,287** | Pre-deduplication |
| **Unique Records (Deduplicated)** | **7,276** | 11 multi-label Takeout duplicates removed |
| **Date Range** | **Aug 2023 → Sep 2026 (38 months)** | Multi-year college lifecycle |
| **Volume by Year** | **2023:** 101 · **2024:** 909 · **2025:** 3,922 · **2026:** 2,344 | Exponential volume growth trajectory |
| **Unread vs. Read** | **3,635 Unread (49.96%)** · 3,641 Read | Concrete proof of student email overload |
| **HTML Fallbacks** | 2,004 records | Required robust CSS & tag stripping |
| **Emails with Attachments** | 657 records (9.0%) | Verified attachments metadata |
| **Top Opportunity Senders** | `dare2compete.news` (934), `sjcem.edu.in` (908), `classroom.google.com` (454), `educative.io` (333), `coursera.org` (260), `eduskillsfoundation.org` (219), `naukri.com` (162) | Verified across Python, MapReduce, Spark, and Hive |

---

## 🌐 The 5 Vs of Big Data (Concrete Mapping)

| Big Data V | Grounded Context in Email Analytics |
|---|---|
| **Volume** | 1.3 GB MBOX / 7,276 records. While medium-scale for a single student, the system is horizontally architected for multi-student campus-wide deployment. Justified by **growth trajectory (101 → 3,922/yr)** and HDFS block distribution. |
| **Velocity** | Handled via periodic/incremental batch drop ingestion. Incoming emails or semester Takeout archives are appended and processed without schema breaks or cluster downtime. |
| **Variety** | Heterogeneous structures: RFC 822 headers, folded MIME labels, multipart/alternative text/HTML, attachment metadata, and unstructured body text. Storage combines JSONL (MapReduce) and columnar Parquet (Spark/Hive). |
| **Veracity** | Real email noise: RFC 2047 base64 encoded subjects, folded `Category\n updates` headers, CSS styling fragments (`table{...}`) from 2,004 HTML bodies, and duplicate message IDs. Preprocessing enforces strict sanitization and conservative date extraction (never guessing). |
| **Value** | Converts 3,635 unread emails of noise into **2,306 categorized career opportunities**, identifies technical domains (691 AI/ML signals), and surfaces **52 active application deadlines**. |

---

## 🏗️ End-to-End Pipeline Architecture

```
  +-------------------------------------------------------------------------+
  |              Google Takeout: 1.3 GB Raw MBOX (7,287 emails)             |
  +-------------------------------------------------------------------------+
                                       |
                                       v
                     [Phase 1] Python Streaming Parser
                                       |
                     +-----------------+-----------------+
                     |                                   |
                     v                                   v
             HDFS: raw/structured/               HDFS: cleaned/
                emails.jsonl                     emails.parquet
                     |                                   |
                     v                                   v
            [Phase 3] Java MapReduce             [Phase 4] Apache Spark (PySpark)
            Batch Domain-Month Sum               - Text Preprocessing & Cleaning
            (results/domain_month_counts)        - Weak Supervision (72.3% Rules)
                                                 - Spark MLlib TF-IDF (HashingTF+IDF)
                                                 - Multinomial Logistic Regression
                                                 - Phase 5: Opportunity Extraction
                                                         |
                                                         v
                                                 HDFS: features/
                                                 emails.parquet (7,276)
                                                         |
                                                         v
                                                 [Phase 6] Apache Hive
                                                 External Table & 7 HQL Queries
                                                         |
                                                         v
                                              data/output/analytics/
                                              Materialized Result Parquet/CSVs
                                                         |
                                                         v
                                                 [Phase 7] Streamlit
                                                 Personalized Student Dashboard
```

---

## 🛠️ Technology Stack & Role Mapping

Every technology is chosen to fulfill a distinct, defensible role in the Big Data lifecycle:

| Layer | Technology | Version | Specific Project Purpose |
|---|---|---|---|
| **Data Ingestion** | Python `mailbox` + `email` | 3.12.4 | Streaming decode of RFC 822 MIME headers, deduplication by Message-ID, and UTC date normalization. |
| **Distributed Storage** | Apache Hadoop HDFS | 3.4.1 | Distributed raw archive, structured JSONL, and columnar Parquet storage across cluster blocks. |
| **Distributed Batch** | Hadoop MapReduce (Java) | Java 11 / Hadoop 3.4.1 | Native distributed batch aggregation computing email counts per `sender_domain × month`. |
| **In-Memory ML** | Apache Spark / PySpark MLlib | 3.5.6 | Distributed text cleaning, TF-IDF feature hashing (`HashingTF` + `IDF`), and Logistic Regression training. |
| **SQL Analytics** | Apache Hive / Spark SQL | 4.0.1 / Derby | External table schema registry over HDFS Parquet and execution of 7 analytical queries. |
| **MLOps & Governance** | MLflow | 3.16.1 | Experiment tracking, model registry, artifact versioning, and automated production gates. |
| **Serving & UI** | Streamlit + Plotly | 1.64.0 / 7.1.0 | Fast, interactive dashboard reading materialized Parquet/CSV files with zero live cluster latency. |

---

## 📂 Repository Structure

```
.
├── BDA_Email_Project_Context.md    # Initial problem statement & project specification
├── implementation_plan.md          # Phased engineering roadmap and milestones
├── README.md                       # Comprehensive project documentation
├── parser/                         # Phase 1: MBOX Parser
│   ├── parse_mbox.py               # Streaming MBOX parser & validator
│   └── requirements.txt            # Parser Python dependencies
├── mapreduce/                      # Phase 3: Java Hadoop MapReduce
│   ├── src/com/bda/email/*.java    # Mapper, Reducer, and Driver source files
│   ├── build.sh                    # Java 11 compiler script
│   ├── run.sh                      # YARN job submission runner
│   ├── domain-month-count.jar      # Compiled MapReduce binary
│   └── README.md                   # MapReduce architecture & validation
├── spark/                          # Phase 4 & 5: Spark MLlib & Extraction
│   ├── __init__.py                 # Spark package
│   ├── preprocess.py               # Regex text cleaning & normalization UDF
│   ├── labeling.py                 # Weak supervision labeling hierarchy
│   ├── classifier.py               # MLlib TF-IDF pipeline & Logistic Regression
│   ├── extract.py                  # Opportunity extraction (campus, deadline, org)
│   ├── pipeline.py                 # End-to-end distributed batch driver
│   ├── run.sh                      # Production Spark runner script
│   └── README.md                   # ML benchmark, schema, & viva cheatsheet
├── hive/                           # Phase 6: Apache Hive Analytics
│   ├── create_tables.sql           # DDL for external table over HDFS Parquet
│   ├── queries.sql                 # 7 analytical HQL queries
│   ├── run_analytics.py            # Query execution & dual-file materializer
│   ├── run.sh                      # Hive runner script
│   └── README.md                   # Query insights & materialization guide
├── dashboard/                      # Phase 7: Streamlit Dashboard
│   ├── __init__.py                 # Dashboard package
│   ├── data_loader.py              # Cached Parquet/CSV data loader
│   ├── app.py                      # Interactive multi-tab dashboard
│   ├── run_dashboard.sh            # Dashboard launch script
│   └── README.md                   # User manual & presentation guide
├── data/output/                    # Data artifacts directory
│   ├── structured/emails.jsonl     # Parser output (MapReduce input)
│   ├── cleaned/emails.parquet      # Parser output (Spark input)
│   ├── features/emails.parquet     # Spark ML output (Hive input)
│   ├── analytics/                  # Hive materialized query outputs (CSV & Parquet)
│   ├── parse_summary.json          # Phase 1 parse metrics
│   └── features_summary.json       # Phase 4 ML execution metrics
├── metadata/                       # Source raw mailbox archive
│   └── All mail Including Spam and Trash.mbox
└── docs/                           # Environment & Progress tracking
    └── ENVIRONMENT_AND_PROGRESS.md
```

---

## 🚀 Step-by-Step Execution Guide

### Prerequisites
Ensure Hadoop, HDFS, YARN, Spark, and Hive are configured with **Java 11**:
```bash
# Start Hadoop HDFS and YARN daemons
start-dfs.sh
start-yarn.sh
jps  # Confirm NameNode, DataNode, ResourceManager, NodeManager are running
```

---

### Phase 1: Parse MBOX Archive
Processes the 1.3 GB MBOX archive into structured JSONL and cleaned Parquet in ~33 seconds:
```bash
python3 parser/parse_mbox.py
```
*Outputs: `data/output/structured/emails.jsonl` (33.7 MB) and `data/output/cleaned/emails.parquet` (14.3 MB).*

---

### Phase 2: Ingest into HDFS
Stage raw and cleaned data into distributed HDFS storage:
```bash
hdfs dfs -mkdir -p /user/student/email_project/{raw/mbox,raw/structured,cleaned,features,results}
hdfs dfs -put -f metadata/All\ mail\ Including\ Spam\ and\ Trash.mbox /user/student/email_project/raw/mbox/
hdfs dfs -put -f data/output/structured/emails.jsonl /user/student/email_project/raw/structured/
hdfs dfs -put -f data/output/cleaned/emails.parquet /user/student/email_project/cleaned/
```

---

### Phase 3: Run Java Hadoop MapReduce Job
Compiles and submits the distributed domain-month aggregation to YARN:
```bash
bash mapreduce/build.sh
bash mapreduce/run.sh
```
*Results: 1,113 unique `domain × month` keys across 7,276 records verified on HDFS at `/user/student/email_project/results/domain_month_counts/`.*

---

### Phase 4 & 5: Spark MLlib Classification & Metadata Extraction
Executes text normalization, TF-IDF feature extraction, weak supervision, Logistic Regression classification, and opportunity extraction:
```bash
bash spark/run.sh
```
*Results: 90.49% test accuracy; outputs 27-column enriched dataset to HDFS `/user/student/email_project/features/emails.parquet`.*

---

### Phase 6: Apache Hive SQL Analytics & Materialization
Creates external table over HDFS Parquet, runs the 7 analytical queries, and materializes results into CSV & Parquet:
```bash
bash hive/run.sh
```
*Outputs: 7 pre-computed analytical datasets stored in `data/output/analytics/`.*

---

### Phase 7: Launch Streamlit Dashboard
Launches the interactive web dashboard on port `8501`:
```bash
bash dashboard/run_dashboard.sh 8501
```
Open **`http://localhost:8501`** in your browser to interact with the visualizations, opportunity roster, and ML inspector.

---

## 📈 Key Analytical Findings & Benchmark Results

### 1. Classification Performance (Spark MLlib)
- **Model:** Multinomial Logistic Regression (`family="multinomial"`, `regParam=0.01`, `maxIter=25`)
- **Accuracy:** **90.49%**
- **Weighted F1 Score:** **90.25%**
- **Weighted Precision:** **90.55%**
- **Weighted Recall:** **90.49%**
- **Hybrid Strategy:** High-confidence weak supervision rules resolve **72.3%** of records with 100% confidence; Spark MLlib predicts the remaining **27.7%**.

### 2. Category Distribution Across Inbox

| Category | Email Count | Share (%) | Description |
|---|---|---|---|
| **Newsletter/Promotional** | 2,951 | 40.56% | Commercial & platform updates |
| **Academic** | 1,701 | 23.38% | Coursework, assignments, classroom notices |
| **Hackathon/Competition** | 1,069 | 14.69% | Unstop coding challenges & competitions |
| **Internship** | 535 | 7.35% | EduSkills, Internshala, college drives |
| **Interview** | 310 | 4.26% | Shortlists, interview calls, schedules |
| **Job/Placement** | 266 | 3.66% | Naukri listings & campus placement drives |
| **Miscellaneous** | 180 | 2.47% | Social media & administrative system notifications |
| **Workshop/Event** | 126 | 1.73% | Tech talks, webinars, bootcamps |
| **Administrative** | 97 | 1.33% | Fee receipts, concessions, bonafide notices |
| **Examination** | 23 | 0.32% | Hall tickets, exam timetables, revaluation notices |
| **Scholarship** | 18 | 0.25% | Financial aid & fellowship announcements |
| **Total** | **7,276** | **100.0%** | **Row Count Invariant Maintained ✅** |

### 3. Student Inbox Fatigue vs. Prioritization
- **Newsletters/Promotions:** **66.5% Unread** (primary source of student email fatigue)
- **Hackathons:** **68.0% Unread** (frequent automated contest updates)
- **Internships:** **83.7% Read** (heavily prioritized by the student)
- **Academic Notices:** **80.5% Read** (critical coursework messages)
- **Examinations:** **82.6% Read** (high-stakes deadlines)

---

## 🎓 Viva-Defensibility Cheatsheet

### 1. Why HDFS for a 1.3 GB dataset?
7.3K emails from one student is a prototype. In a university deployment across thousands of students, email archives grow to hundreds of gigabytes rapidly. HDFS provides fault-tolerant distributed block storage, data locality for MapReduce/Spark, and eliminates local disk bottlenecks.

### 2. Why Java MapReduce when Spark is already used?
MapReduce demonstrates low-level distributed batch computing (Map, Combine, Shuffle, Reduce) and fulfills the academic coursework Java MapReduce requirement. Reducer reuse as Combiner showcases map-side shuffle optimization.

### 3. Why Weak Supervision instead of manual labeling or LLMs?
Manual labeling of 7,276 emails is infeasible for students. LLM APIs introduce rate limits, external API costs, and privacy concerns for personal emails. Rule-based weak supervision seeds high-precision labels (72.3%), and Spark MLlib generalizes over ambiguous cases (27.7%) in seconds with full explainability.

### 4. Why HashingTF instead of CountVectorizer?
`HashingTF` uses MurmurHash3 to map tokens directly to 20,000 feature buckets without maintaining an in-memory dictionary. This avoids expensive cluster-wide dictionary broadcasts and shuffle stages across distributed executors.

### 5. Why Hive External Tables instead of Managed Tables?
An `EXTERNAL` table ensures Hive acts purely as a schema registry and query engine over HDFS Parquet files. If the table is dropped, the HDFS dataset remains 100% intact. Furthermore, Parquet allows Spark, Hive, Beeline, and Streamlit to access the same storage layer without format translation.

### 6. Why materialize Hive query results for Streamlit?
In enterprise Big Data architectures (Lambda Architecture serving layer), the analytical processing engine precomputes aggregates into a fast serving layer. Reading small Parquet/CSV files enables sub-second UI responsiveness, completely removing runtime dependencies on active Hadoop/Hive cluster services during live evaluation.

---

## 👥 Team & Coursework Attribution

- **Course:** Big Data Analytics (BDA) Laboratory / Mini-Project
- **Architecture:** BDA-First Distributed Pipeline (HDFS, MapReduce, PySpark, Hive, Streamlit)
- **Dataset Origin:** Developer's real college Gmail account export (Google Takeout, 2023–2026)
- **Status:** **All Phases 1–7 Complete & Validated**