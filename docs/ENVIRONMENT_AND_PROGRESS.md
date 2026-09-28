# Environment State & Progress Summary

**Date:** 2026-09-18  
**Project:** Intelligent College Email Classification and Opportunity Analytics System  
**Status:** All Phases 1–7 Complete | End-to-End Pipeline & Dashboard Ready for Demo

---

## Critical Environment Configuration

### System Audit Results (2026-09-17)

**All components installed and verified working:**

| Component | Version | Location | Java | Status |
|---|---|---|---|---|
| Java (runtime) | OpenJDK 11.0.24 | `/usr/bin/java` → Java 11 | — | ✅ Active |
| Java (compiler) | OpenJDK 21.0.4 | `/usr/bin/javac` → Java 21 | — | ⚠️ See note below |
| Hadoop | 3.4.1 | `/usr/local/hadoop` | Java 11 | ✅ Running |
| HDFS | 3.4.1 | (Hadoop bins) | Java 11 | ✅ Running |
| YARN | 3.4.1 | (Hadoop bins) | Java 11 | ✅ Running |
| Spark | 3.5.6 | `/opt/spark` | Java 11 | ✅ Configured |
| PySpark | 3.5.6 | `/opt/spark/python/pyspark/` | Java 11 | ✅ Available |
| Python | 3.12.4 | `/usr/local/bin/python3` | — | ✅ Compatible with PySpark 3.5.x |
| Hive | 4.0.1 | `/usr/local/hive` | Java 11 | ✅ Metastore initialized (Derby) |
| Beeline | (Hive 4.0.1) | `/usr/local/hive/bin/beeline` | Java 11 | ✅ Available |
| Apache Flume | 1.11.0 | `/usr/local/flume` | Java 11 | ✅ (Not used in this project) |

**Environment Variables (verified in ~/.bashrc):**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_MAPRED_HOME=/usr/local/hadoop
export SPARK_HOME=/opt/spark
export HIVE_HOME=/usr/local/hive
export FLUME_HOME=/usr/local/flume
```

**Python Packages Installed (pip3):**
- html2text==2025.4.15
- python-dateutil==2.9.0.post0
- pyarrow==25.0.1
- pandas==3.0.6
- numpy==2.5.3
- six==1.17.0

### Critical Fix Applied

**Java Compiler Mismatch:**
- System's default `javac` resolves to **Java 21** via `update-alternatives`
- Hadoop/Spark/Hive all run on **Java 11**
- **Solution:** MapReduce compilation explicitly uses `/usr/lib/jvm/java-11-openjdk-amd64/bin/javac --release 11` (see `mapreduce/build.sh`)

**Spark Configuration:**
- Created `/opt/spark/conf/spark-env.sh` with:
  ```bash
  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
  export HADOOP_HOME=/usr/local/hadoop
  export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
  ```

**Hadoop Config Fix:**
- Fixed malformed `/usr/local/hadoop/etc/hadoop/yarn-site.xml` (had nested `<configuration>` tags)
- Backup saved as `yarn-site.xml.bak`

### HDFS/YARN State

**Must restart after reboot:**
```bash
start-dfs.sh
start-yarn.sh
```

**Current daemons (verified running):**
- NameNode, DataNode, SecondaryNameNode
- ResourceManager, NodeManager

**HDFS Status:**
- 92.88 GB capacity available
- Replication factor: 1 (pseudo-distributed single-node)
- NameNode formatted (clusterID exists)

---

## Dataset Statistics (Phase 1 Validated)

**Source:** `metadata/All mail Including Spam and Trash.mbox` (1.3 GB)

| Metric | Value |
|---|---|
| Raw messages (with duplicates) | 7,287 |
| **Unique records (deduplicated by Message-ID)** | **7,276** |
| Duplicates removed | 11 |
| Missing subject | 76 |
| Missing date | 0 |
| HTML fallback (no plain text) | 2,004 |
| Empty body | 124 |
| With attachments | 657 |
| Unread | 3,635 |
| Sent | 322 |
| Spam | 26 |
| Important | 2,091 |
| Date range | Aug 2023 → Sep 2026 |
| Volume by year | 2023:101, 2024:909, 2025:3,922, 2026:2,344 |

**Top Sender Domains (Exact Counts — use for validation):**

| Domain | Count | Meaning |
|---|---|---|
| dare2compete.news | 934 | Unstop (hackathons/competitions) |
| sjcem.edu.in | 908 | College (academic/admin) |
| classroom.google.com | 454 | Academic coursework |
| educative.io | 333 | Learning platform |
| m.learn.coursera.org | 260 | Coursera |
| mail.verywellmind.com | 232 | Newsletter |
| eduskillsfoundation.org | 219 | Internships |
| email.microsoft.com | 191 | Microsoft |
| datacamp.com | 172 | Learning platform |
| naukri.com | 162 | Jobs/placements |

**Unique domains:** 183

---

## HDFS Project Structure (Current State)

```
hdfs:///user/student/email_project/
├── raw/
│   ├── mbox/
│   │   └── All_mail_Including_Spam_and_Trash.mbox    1.2 GB (provenance)
│   └── structured/
│       └── emails.jsonl                              33.7 MB (MapReduce input)
├── cleaned/
│   └── emails.parquet                                14.3 MB (Spark input)
├── features/
│   (empty — Phase 4 writes classified data here)
└── results/
    └── domain_month_counts/
        ├── _SUCCESS
        └── part-r-00000                              29 KB (1,113 rows)
```

**Schema of `emails.jsonl` and `emails.parquet` (17 columns):**
```
message_id, thread_id, sender, sender_email, sender_domain,
recipients (array), subject, date_utc, body_text, body_length,
has_attachment, attachment_count, gmail_labels (array),
is_unread, is_spam, is_sent, is_important
```

---

## Phase 1 (Parser) — COMPLETE ✅

**Files:**
- `parser/parse_mbox.py` (315 lines)
- `parser/requirements.txt`
- `data/output/structured/emails.jsonl` (33.7 MB)
- `data/output/cleaned/emails.parquet` (14.3 MB)
- `data/output/parse_summary.json`

**Validation Results:**
- ✅ 7,276 unique records (≥ 7,100 threshold)
- ✅ RFC 2047 subject decoding working
- ✅ Gmail label parsing handles folded lines (`Category\n updates` → `Category updates`)
- ✅ Dates normalized to UTC ISO-8601
- ✅ Body extraction: plain text preferred, html2text fallback
- ✅ Zero null values in `message_id`, `sender_domain`, `date_utc`, `body_text`
- ✅ Parquet schema validated (17 columns, snappy compression)

**Runtime:** ~33 seconds for 1.3 GB MBOX

---

## Phase 2 (HDFS Ingestion) — COMPLETE ✅

**Actions:**
- Created HDFS directory structure
- Uploaded `All_mail_Including_Spam_and_Trash.mbox` → `/user/student/email_project/raw/mbox/`
- Uploaded `emails.jsonl` → `/user/student/email_project/raw/structured/`
- Uploaded `emails.parquet` → `/user/student/email_project/cleaned/`

**Spark Read Test:**
```python
df = spark.read.parquet("hdfs://localhost:9000/user/student/email_project/cleaned/emails.parquet")
df.count()  # 7276 ✅
```

---

## Phase 3 (Java MapReduce) — COMPLETE ✅

**Job:** "Emails per sender-domain per month"

**Files:**
- `mapreduce/src/com/bda/email/DomainMonthCountMapper.java`
- `mapreduce/src/com/bda/email/DomainMonthCountReducer.java`
- `mapreduce/src/com/bda/email/DomainMonthCountDriver.java`
- `mapreduce/build.sh` (compiles with Java 11)
- `mapreduce/run.sh` (submits to YARN)
- `mapreduce/README.md` (viva documentation)
- `mapreduce/domain-month-count.jar`

**Input:** `/user/student/email_project/raw/structured/emails.jsonl` (JSONL via TextInputFormat)

**Output:** `/user/student/email_project/results/domain_month_counts/part-r-00000`

**Results Validated:**
- 1,113 unique `domain × month` keys
- Grand total: 7,276 emails (matches Phase 1 exactly)
- **Cross-check:** All top-5 domain counts match Phase 1 Python computation exactly:
  - dare2compete.news: 934 ✅
  - sjcem.edu.in: 908 ✅
  - classroom.google.com: 454 ✅
  - eduskillsfoundation.org: 219 ✅
  - naukri.com: 162 ✅

**Counters (Hadoop job report):**
```
RecordCounter
    EMITTED=7276
    MALFORMED_JSON=0
    MISSING_DOMAIN=0
    MISSING_DATE=0
```

**Runtime:** ~13 seconds on YARN

**Build/Run Commands:**
```bash
bash mapreduce/build.sh
bash mapreduce/run.sh
```

---

## Phase 4 (Spark) — COMPLETE ✅

**Job:** Distributed Text Preprocessing, TF-IDF, Weak Supervision, Spark MLlib Classification & Feature Generation

**Files:**
- `spark/__init__.py`
- `spark/preprocess.py` (text cleaning, CSS/quoted text/signature stripping, normalization)
- `spark/labeling.py` (weak supervision rules, forum filtering, Gmail bootstrap)
- `spark/classifier.py` (TF-IDF pipeline, LR & NB estimators, evaluation, hybrid resolution)
- `spark/extract.py` (opportunity extraction: campus_type, organization, deadline, keywords)
- `spark/pipeline.py` (end-to-end Spark driver)
- `spark/run.sh` (runner script with Java 11 / Spark environment bindings)
- `spark/README.md` (comprehensive Phase 4 architecture, benchmark, schema & viva docs)
- `data/output/features_summary.json` (execution metrics and distribution report)
- `data/output/features/emails.parquet` (local Parquet output)
- `hdfs://localhost:9000/user/student/email_project/features/emails.parquet` (HDFS Parquet output)

**Validation Results:**
- ✅ Input Count: 7,276 records (matches Phase 1 and Phase 3 exactly)
- ✅ Output Count: 7,276 records (zero records dropped)
- ✅ Weak Supervision Rule Labeled: 5,259 (72.3%)
- ✅ Unassigned (ML Handed): 2,017 (27.7%)
- ✅ MLlib Logistic Regression Test Accuracy: **90.49%**, Weighted F1: **90.25%**
- ✅ MLlib Naive Bayes Benchmark: **86.83%** Accuracy, **87.19%** F1
- ✅ Persisted to HDFS: `hdfs://localhost:9000/user/student/email_project/features/emails.parquet` (19.1 MB snappy Parquet)
- ✅ Clean Parquet Schema: MLlib Vector UDTs removed to ensure 100% compatibility with Apache Hive external tables (Phase 6)
- ✅ All opportunity fields populated: `campus_type`, `organization`, `opportunity_type`, `deadline`, `event_date`, `keywords`
- ✅ Runtime: ~40.4 seconds end-to-end on local[2] Spark master

**Category Distribution:**
- Newsletter/Promotional: 2,951 (40.6%)
- Academic: 1,701 (23.4%)
- Hackathon/Competition: 1,069 (14.7%)
- Internship: 535 (7.4%)
- Interview: 310 (4.3%)
- Job/Placement: 266 (3.7%)
- Miscellaneous: 180 (2.5%)
- Workshop/Event: 126 (1.7%)
- Administrative: 97 (1.3%)
- Examination: 23 (0.3%)
- Scholarship: 18 (0.2%)

---

## Phase 5 (Information Extraction) — COMPLETE ✅

**Job:** Rule-based and heuristic extraction of structured opportunity metadata without guessing.

**Files:**
- `spark/extract.py` (extraction logic for campus_type, organization, opportunity_type, deadline, event_date, keywords)
- Output columns persisted in `hdfs://localhost:9000/user/student/email_project/features/emails.parquet`

**Validation Results (Across 7,276 emails / 2,306 opportunity emails):**
- ✅ `campus_type`: 2,306 / 2,306 opportunity emails identified (100% coverage; 88.6% off-campus, 11.4% on-campus)
- ✅ `organization`: 2,283 / 2,306 opportunity emails mapped (99.0% coverage; Unstop, SJCEM, EduSkills, Naukri, Coursera, etc.)
- ✅ `opportunity_type`: 2,306 / 2,306 categorized into technical sub-domains (AI/ML, Data Science, Web Dev, Cloud, Cyber, SWE)
- ✅ `deadline`: 52 verified deadlines normalized to ISO `YYYY-MM-DD` (never guessed when absent)
- ✅ `keywords`: 1,571 opportunity emails tagged with technology skills (Python, ML, SQL, etc.)

---

## Phase 6 (Hive Analytics) — COMPLETE ✅

**Job:** Hive external table over HDFS Parquet, 7 analytical queries, and result materialization for Streamlit.

**Files:**
- `hive/create_tables.sql` (Hive DDL external table `email_analytics.email_features` over HDFS Parquet)
- `hive/queries.sql` (7 analytical HQL queries)
- `hive/run_analytics.py` (query execution and dual CSV + Parquet materializer)
- `hive/run.sh` (runner script with Java 11 / Spark / Hive environment bindings)
- `hive/README.md` (comprehensive Phase 6 documentation, query results, and viva guide)
- `data/output/analytics/*.csv` & `*.parquet` (materialized result files)
- `data/output/analytics/analytics_summary.json` (top-level summary KPIs)

**Validation Results:**
- ✅ Query 1 (Category count): Exact integer match across all 11 classes with Phase 4 output
- ✅ Query 2 (Month-over-month): Demonstrates Volume/Velocity trajectory (101 in 2023 -> 3,922 in 2025 -> 2,344 in 2026)
- ✅ Query 3 (Opportunity breakdown): Sub-domains quantified (AI/ML leads with 691 opportunities)
- ✅ Query 4 (Campus distribution): On-campus vs. off-campus quantified across all categories
- ✅ Query 5 (Read vs. unread): High unread on Newsletters (66.5%), high read on Internships (83.7%) and Academics (80.5%)
- ✅ Query 6 (Top senders): Matches MapReduce Phase 3 and Phase 1 exactly (dare2compete.news: 934, sjcem: 908, classroom: 454)
- ✅ Query 7 (Deadlines): 52 active/historical deadlines cataloged chronologically
- ✅ Runtime: ~12.2 seconds end-to-end for all 7 queries and dual file materialization

---

## Phase 7 (Streamlit Dashboard) — COMPLETE ✅

**Job:** Interactive, decoupled student-facing web analytics and opportunity dashboard.

**Files:**
- `dashboard/__init__.py`
- `dashboard/data_loader.py` (cached Parquet/CSV data loader)
- `dashboard/app.py` (Streamlit web application with Plotly interactive charts)
- `dashboard/run_dashboard.sh` (dashboard launcher script)
- `dashboard/README.md` (comprehensive Phase 7 user manual and viva cheatsheet)

**Features Verified:**
- ✅ 6 Summary KPI Cards (7,276 total emails, 3,635 unread [49.96%], 2,306 opportunities [31.7%], 535 internships, 1,069 hackathons, 52 deadlines)
- ✅ Tab 1 (Inbox Overview & Trends): Category distribution bar chart, month-over-month volume area chart, read vs unread stacked priority chart, on/off-campus ratio pie, top 15 senders bar chart
- ✅ Tab 2 (Opportunity Explorer): Multi-filter table (`Opportunity | Category | Type | Organization | Campus | Deadline`) with sub-domain, campus sourcing, read status filters, keyword search, and CSV export
- ✅ Tab 3 (Deadlines & Calendar): Chronological table of 52 verified ISO `YYYY-MM-DD` deadlines with category and organization breakdown
- ✅ Tab 4 (Email & ML Inspector): Search across all 7,276 emails, inspect ML confidence, filter by prediction method (`rule` 72.3% vs `ml` 27.7%), and inspect NLP preprocessed tokens
- ✅ Tab 5 (Big Data 5 Vs & Architecture): Comprehensive viva cheatsheet, ASCII data flow diagram, and technology comparison table
- ✅ Launch Verified: Tested via `streamlit run dashboard/app.py --server.port=8501`, HTTP 200 response verified

---

## Files Present in Workspace

```
/home/student/Aditya BDA Project/
├── .gitignore
├── BDA_Email_Project_Context.md
├── implementation_plan.md
├── README.md
├── parser/
│   ├── parse_mbox.py
│   └── requirements.txt
├── mapreduce/
│   ├── src/com/bda/email/*.java
│   ├── build.sh
│   ├── run.sh
│   ├── README.md
│   ├── domain-month-count.jar
│   └── build/ (compiled .class files)
├── spark/
│   ├── __init__.py
│   ├── preprocess.py
│   ├── labeling.py
│   ├── classifier.py
│   ├── extract.py
│   ├── pipeline.py
│   ├── run.sh
│   └── README.md
├── hive/
│   ├── create_tables.sql
│   ├── queries.sql
│   ├── run_analytics.py
│   ├── run.sh
│   └── README.md
├── dashboard/
│   ├── __init__.py
│   ├── data_loader.py
│   ├── app.py
│   ├── run_dashboard.sh
│   └── README.md
├── data/
│   └── output/
│       ├── structured/emails.jsonl

│       ├── cleaned/emails.parquet
│       ├── features/emails.parquet
│       ├── analytics/*.csv & *.parquet
│       ├── parse_summary.json
│       ├── features_summary.json
│       └── analytics/analytics_summary.json
├── metadata/
│   └── All mail Including Spam and Trash.mbox (1.3 GB)
└── docs/
    └── ENVIRONMENT_AND_PROGRESS.md
```

---

## Key Decisions/Conventions

1. **No external JARs for MapReduce:** Jackson JSON parser already bundled in Hadoop 3.4.1 classpath
2. **JSONL for MapReduce, Parquet for Spark:** TextInputFormat vs columnar efficiency
3. **Combiner optimization:** Reducer reused as Combiner (reduces shuffle I/O)
4. **Counter-based Veracity handling:** Malformed records skipped, not failed
5. **Java 11 compilation explicit:** Bypasses system's Java 21 `javac` default
6. **Spark reads HDFS via `hdfs://localhost:9000`** (verified working)
7. **Hive metastore:** Derby embedded at `/home/student/hive_metastore_db`

---

## Known Issues / Notes

1. **HDFS/YARN not persistent across reboots** — must manually `start-dfs.sh && start-yarn.sh`
2. **PySpark not pip-installed** — use via `SPARK_HOME/bin/pyspark` or add to `PYTHONPATH`
3. **SLF4J warning in Hive** — harmless (multiple bindings), does not affect functionality
4. **HTML body fragments** — 2,004 emails use html2text conversion; some retain CSS noise (Phase 4 preprocessing handles this)

---

## Validation Cross-Reference (Use These Numbers)

**Always cross-check Spark/Hive outputs against these known totals:**

| Metric | Value | Source |
|---|---|---|
| Total unique emails | 7,276 | Phase 1 parser |
| dare2compete.news | 934 | Phase 1 + Phase 3 MR |
| sjcem.edu.in | 908 | Phase 1 + Phase 3 MR |
| classroom.google.com | 454 | Phase 1 + Phase 3 MR |
| Unread emails | 3,635 | Phase 1 parser |
| Sent emails | 322 | Phase 1 parser |
| Spam emails | 26 | Phase 1 parser |
| Emails with attachments | 657 | Phase 1 parser |
| Unique sender domains | 183 | Phase 3 MR output |
| Domain-month pairs | 1,113 | Phase 3 MR output |

---

## Commands for New Session

**Restart Hadoop (after reboot):**
```bash
start-dfs.sh
start-yarn.sh
jps  # verify 5 daemons running
```

**Verify HDFS:**
```bash
hdfs dfs -ls /user/student/email_project/
```

**Re-run MapReduce (if needed):**
```bash
cd "/home/student/Aditya BDA Project"
bash mapreduce/run.sh
```

**Spark shell test:**
```bash
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
SPARK_HOME=/opt/spark \
/opt/spark/bin/pyspark --master local[2]

# In PySpark:
df = spark.read.parquet("hdfs://localhost:9000/user/student/email_project/cleaned/emails.parquet")
df.count()  # Should be 7276
```

---

**End of Context Document**  
**Next Phase:** Spark NLP/ML classification pipeline (Phase 4)
