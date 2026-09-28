# Phase 6 — Apache Hive SQL Analytics Layer

**Intelligent College Email Classification and Opportunity Analytics System**  
**Component:** `hive/`  
**Query Engine:** Apache Hive 4.0.1 / Spark SQL on HDFS Parquet  

---

## 1. Overview & Purpose

Phase 6 establishes the SQL analytical and query layer of the Big Data pipeline.

### Core Objectives:
1. **External Table DDL:** Define an external Hive table (`email_analytics.email_features`) over the semantically enriched Parquet dataset produced in Phase 4 and stored in HDFS (`/user/student/email_project/features/emails.parquet`).
2. **Analytical SQL Queries:** Implement the 7 comprehensive analytical queries answering core academic and opportunity questions:
   - Category distribution
   - Month-over-month volume trajectory (demonstrating Volume & Velocity)
   - Opportunity breakdown across technical sub-domains (AI/ML, Data Science, Web Dev)
   - On-campus vs. Off-campus opportunity distribution
   - Read vs. Unread behavior across message categories (demonstrating student priorities)
   - Top sender domains and recognized recruiting organizations
   - Active and historical opportunity deadlines
3. **Materialization Strategy:** Execute queries and materialize results into small, high-performance CSV and Parquet files under `data/output/analytics/`, enabling instantaneous, zero-latency rendering by the Streamlit dashboard in Phase 7.

---

## 2. Architecture & Data Flow

```
+-------------------------------------------------------------------------+
|                  HDFS: features/emails.parquet (7,276)                  |
+-------------------------------------------------------------------------+
                                    |
                                    v
           [1] Hive External Table (hive/create_tables.sql)
               Database: email_analytics
               Table:    email_features
               Storage:  STORED AS PARQUET
               Location: /user/student/email_project/features/emails.parquet
                                    |
                                    v
           [2] Analytical SQL Queries (hive/queries.sql)
               1. Category Distribution
               2. Month-over-Month Growth (2023 -> 2026)
               3. Opportunity Technical Sub-Domains
               4. On-Campus vs Off-Campus Ratio
               5. Read vs Unread Patterns
               6. Top 25 Sender Domains & Organizations
               7. Filterable Opportunity Deadlines
                                    |
                                    v
           [3] Result Materialization (hive/run_analytics.py)
               Materializes to data/output/analytics/:
               - category_counts.csv / .parquet
               - monthly_volume.csv / .parquet
               - opportunity_breakdown.csv / .parquet
               - campus_distribution.csv / .parquet
               - read_unread_distribution.csv / .parquet
               - top_senders.csv / .parquet
               - upcoming_deadlines.csv / .parquet
               - analytics_summary.json
                                    |
                                    v
+-------------------------------------------------------------------------+
|             Phase 7 Streamlit Dashboard (Sub-second loading)            |
+-------------------------------------------------------------------------+
```

---

## 3. Directory Layout

```
hive/
├── create_tables.sql     # Hive DDL script for external table creation
├── queries.sql           # All 7 analytical HQL queries
├── run_analytics.py      # Execution engine and result materializer
├── run.sh                # Production runner script with environment bindings
└── README.md             # Architecture, query results & viva cheatsheet
```

---

## 4. The 7 Analytical Queries & Key Insights

### Query 1: Email Count and Share by Category
*Answers: What is the high-level distribution of messages in the inbox?*

| Category | Email Count | Percentage |
|---|---|---|
| **Newsletter/Promotional** | 2,951 | 40.56% |
| **Academic** | 1,701 | 23.38% |
| **Hackathon/Competition** | 1,069 | 14.69% |
| **Internship** | 535 | 7.35% |
| **Interview** | 310 | 4.26% |
| **Job/Placement** | 266 | 3.66% |
| **Miscellaneous** | 180 | 2.47% |
| **Workshop/Event** | 126 | 1.73% |
| **Administrative** | 97 | 1.33% |
| **Examination** | 23 | 0.32% |
| **Scholarship** | 18 | 0.25% |

---

### Query 2: Month-over-Month Volume Trend (2023 → 2026)
*Answers: How is email volume growing over time, and when do opportunities arrive?*
- **2023:** 101 total emails (baseline student start).
- **2024:** 909 emails (steady growth to 218 in September).
- **2025:** 3,922 emails (peaked in May 2025 with 455 emails/month).
- **2026:** 2,344 emails (averaging ~280–350/month through September).
*Directly proves the Big Data **Volume** and **Velocity** justification: the system handles an exponential growth trajectory.*

---

### Query 3: Opportunity Category & Technical Sub-Domains
*Answers: What types of technical opportunities are available?*

| Category | Total | AI/ML | Data Science | Web Dev | Cloud & DevOps | Cyber | Software Eng |
|---|---|---|---|---|---|---|---|
| **Hackathon/Competition** | 1,069 | 341 | 56 | 55 | 25 | 15 | 65 |
| **Internship** | 535 | 116 | 12 | 2 | 4 | 18 | 13 |
| **Interview** | 310 | 114 | 3 | 36 | 8 | 5 | 98 |
| **Job/Placement** | 266 | 37 | 1 | 3 | 6 | 54 | 28 |
| **Workshop/Event** | 126 | 83 | 1 | 2 | 10 | 0 | 3 |

*AI/ML dominates opportunity topics (691 total occurrences), followed by Software Engineering and Web Development.*

---

### Query 4: On-Campus vs. Off-Campus Opportunities
*Answers: Are student opportunities sourced directly by the college or externally?*

| Category | Off-Campus | On-Campus | % Off-Campus | % On-Campus |
|---|---|---|---|---|
| **Academic** | 1,013 | 688 | 59.6% | 40.4% |
| **Hackathon/Competition** | 1,047 | 22 | 97.9% | 2.1% |
| **Internship** | 423 | 112 | 79.1% | 20.9% |
| **Interview** | 299 | 11 | 96.5% | 3.5% |
| **Job/Placement** | 255 | 11 | 95.9% | 4.1% |
| **Workshop/Event** | 110 | 16 | 87.3% | 12.7% |

*Insight: Hackathons, jobs, and interviews are overwhelmingly sourced from external industry portals (Unstop, Naukri, LinkedIn), whereas 20.9% of internships and 40.4% of academic notices originate directly on-campus.*

---

### Query 5: Read vs. Unread Distribution Across Categories
*Answers: What kinds of emails does the student actually open versus ignore?*

| Category | Total | Unread | Read | Unread % |
|---|---|---|---|---|
| **Newsletter/Promotional** | 2,951 | 1,963 | 988 | **66.5%** |
| **Academic** | 1,701 | 331 | 1,370 | **19.5%** |
| **Hackathon/Competition** | 1,069 | 727 | 342 | **68.0%** |
| **Internship** | 535 | 87 | 448 | **16.3%** |
| **Job/Placement** | 266 | 114 | 152 | **42.9%** |
| **Interview** | 310 | 170 | 140 | **54.8%** |
| **Examination** | 23 | 4 | 19 | **17.4%** |

*Critical Finding: High-value opportunities like **Internships (83.7% read)**, **Academic notices (80.5% read)**, and **Examinations (82.6% read)** are heavily prioritized by the student, whereas generic Newsletters (66.5% unread) and frequent Hackathons (68.0% unread) cause severe inbox overload.*

---

### Query 6: Top Sender Domains & Organizations
*Answers: Who sends the most emails, and who provides the most opportunities?*

1. **`dare2compete.news` (Unstop):** 934 total emails (928 opportunities)
2. **`sjcem.edu.in` (SJCEM College):** 908 total emails (172 opportunities)
3. **`classroom.google.com` (Google Classroom):** 454 total emails
4. **`educative.io` (Educative):** 333 total emails
5. **`m.learn.coursera.org` (Coursera):** 260 total emails
6. **`eduskillsfoundation.org` (EduSkills):** 219 total emails (219 internships)
7. **`naukri.com` (Naukri):** 162 total emails (162 jobs)

---

### Query 7: Active & Filterable Deadlines
*Answers: What deadlines must the student act upon?*
- Extracted 52 explicit deadlines normalized to ISO `YYYY-MM-DD`.
- Examples include campus organizer applications, robotics sprints, Tally Code Brewers, and DRDO hiring drives.

---

## 5. How to Run

### Run Full Analytics Suite
```bash
bash hive/run.sh
```

### Run Raw Queries via Spark SQL
```bash
/opt/spark/bin/spark-sql -f hive/queries.sql
```

---

## 6. Viva-Defensibility Cheatsheet

### Q: Why use an EXTERNAL table instead of a MANAGED (internal) table in Hive?
- **Data Independence & Safety:** Managed tables delete the underlying HDFS data if the table is dropped. An `EXTERNAL` table ensures Hive acts purely as a schema registry and query engine over HDFS Parquet files. If the table is dropped, the HDFS dataset remains 100% intact.
- **Multiple Consumers:** The external table points to `/user/student/email_project/features/emails.parquet`, which can be queried simultaneously by Hive, PySpark, Beeline, and downstream tools without ownership conflicts.

### Q: Why store Hive tables as PARQUET instead of TEXTFILE or ORC?
- **Columnar Efficiency:** Queries like `SELECT category, count(*) ...` only scan the `category` column (a few kilobytes), skipping the massive `body_text` and `cleaned_text` columns.
- **Compression & Speed:** Parquet with Snappy compression reduces disk footprint while enabling fast in-memory decompression.
- **Cross-Framework Standard:** Parquet is natively understood by Spark, Hive, Presto, DuckDB, and Pandas/PyArrow.

### Q: Why materialize Hive results to CSV/Parquet for Streamlit instead of querying Hive live?
- **Performance & Reliability:** Querying HiveServer2 live from a web dashboard introduces 2–5 seconds of JDBC/Thrift overhead per chart click.
- **Defensibility:** Pre-aggregating batch metrics into small result files is the standard Big Data serving layer pattern (Lambda Architecture serving layer). Streamlit reads these files in sub-milliseconds with `pandas.read_parquet()`, making the demo rock-solid even if Hive services are restarting.
