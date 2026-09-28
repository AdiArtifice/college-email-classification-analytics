# Phase 7 — Streamlit Opportunity & Analytics Dashboard

**Intelligent College Email Classification and Opportunity Analytics System**  
**Component:** `dashboard/`  
**Engine:** Streamlit 1.64.0 + Plotly 7.1.0 on Python 3.12  

---

## 1. Overview & Purpose

Phase 7 provides the interactive, student-facing presentation and exploration layer of the Big Data pipeline.

### Core Objectives:
1. **Serving Layer Decoupling:** In alignment with the Big Data **Lambda Architecture** serving pattern, Streamlit reads pre-materialized Parquet/CSV datasets produced by the Phase 6 Hive analytics queries (`data/output/analytics/`) and the full enriched dataset (`data/output/features/emails.parquet`).
2. **Zero Runtime Dependency on Cluster Services:** The dashboard avoids a live Thrift/HiveServer2 connection during demos. This eliminates JVM connection latency (saving 2–5 seconds per chart reload) and guarantees 100% demo stability even if Hadoop/YARN/Hive daemons are restarting.
3. **Core Student Value:** Directly answers the two defining questions:
   - *What is happening in my email inbox?* (Overview, volume growth, fatigue patterns)
   - *What important career opportunities must I know about?* (Filterable table, deadline calendar)

---

## 2. Architecture & Data Flow

```
  +-------------------------------------------------------------------------+
  |              HDFS: features/emails.parquet (7,276 records)              |
  +-------------------------------------------------------------------------+
                                       |
                                       v
                    [Phase 6] Apache Hive SQL Queries
                                       |
                                       v
                     data/output/analytics/ (Materialized)
                     - category_counts.parquet / .csv
                     - monthly_volume.parquet / .csv
                     - opportunity_breakdown.parquet / .csv
                     - campus_distribution.parquet / .csv
                     - read_unread_distribution.parquet / .csv
                     - top_senders.parquet / .csv
                     - upcoming_deadlines.parquet / .csv
                     - analytics_summary.json
                                       |
                                       v
                    [Phase 7] Streamlit Dashboard (dashboard/app.py)
                    - Cached with @st.cache_data for instant reloads
                    - Interactive Plotly charts & dynamic filtering
                    - Export to CSV capabilities
```

---

## 3. Directory Layout

```
dashboard/
├── __init__.py           # Dashboard package definition
├── data_loader.py        # Cached data loader module (Parquet & CSV fallbacks)
├── app.py                # Main Streamlit web application
├── run_dashboard.sh      # Bash runner script
└── README.md             # Architecture, user manual & viva guide
```

---

## 4. Key Views & Features

### 1. Executive Summary KPI Cards
- **Total Emails:** `7,276` (100% parsed from 1.3 GB MBOX)
- **Unread Inflow:** `3,635` (49.96% inbox fatigue rate)
- **Total Opportunities:** `2,306` (31.7% high-value signals)
- **Internships:** `535` (EduSkills, Internshala, college drives)
- **Hackathons:** `1,069` (Unstop, coding challenges)
- **Active Deadlines:** `52` (ISO `YYYY-MM-DD` normalized target dates)

### 2. 📊 Tab 1: Inbox Overview & Trends
- **Category Distribution:** Horizontal bar chart showing exact counts across all 11 classes.
- **Month-over-Month Volume (2023 → 2026):** Interactive area chart showing the growth trajectory (101 in 2023 → 3,922 in 2025 → 2,344 in 2026).
- **Read vs. Unread Behavior:** Stacked bar chart proving student fatigue on Newsletters (66.5% unread) vs. extreme prioritization of Internships (83.7% read) and Academics (80.5% read).
- **On-Campus vs. Off-Campus Ratio:** Donut chart showing external industry portals (88.6%) vs. on-campus sourcing (11.4%).
- **Top 15 Sender Domains:** Bar chart highlighting Unstop (934), SJCEM (908), Google Classroom (454), Educative (333), Coursera (260), EduSkills (219), and Naukri (162).

### 3. 🎯 Tab 2: Opportunity Explorer
- Implements the exact user specification:  
  **`Opportunity | Category | Type | Organization | Campus | Deadline | Status`**
- **Dynamic Multi-Select Filters:**
  - Category (Internship, Hackathon, Job, Interview, Workshop)
  - Sub-Domain (AI/ML, Data Science, Web Dev, Cloud & DevOps, Cybersecurity, SWE)
  - Campus Sourcing (On-campus vs. Off-campus)
  - Read Status (All, Unread only, Read only)
  - Keyword / Subject Search
  - "Has Explicit Deadline Only" toggle
- **Export Feature:** One-click CSV export of filtered opportunities (`student_opportunities.csv`).

### 4. ⏰ Tab 3: Deadlines & Events
- Chronological roster of all 52 verified opportunity deadlines.
- Deadline breakdown by Category and Sponsoring Organization.
- Color-coded deadline status and call-to-action details.

### 5. 🔍 Tab 4: Email & ML Inspector
- Real-time search across all 7,276 emails.
- Transparent attribution: distinguishes between **Weak Supervision Rule (72.3%)** and **Spark MLlib Inference (27.7%)**.
- Confidence score slider filter.
- Deep inspect expanders showing:
  - Header metadata & attachment counts
  - Raw original body snippet
  - NLP preprocessed tokens (CSS/URLs/signatures removed, character repetition normalized).

### 6. 🏛️ Tab 5: Big Data 5 Vs & Architecture (Viva Defense)
- Comprehensive viva cheatsheet explaining the project's grounded mapping to:
  - **Volume:** 7.3K records / 1.3 GB, scaling 101 → 3,922/year.
  - **Velocity:** Incremental batch drop ingestion.
  - **Variety:** MBOX headers, plain/HTML text, attachments, labels.
  - **Veracity:** RFC 2047 decoding, folded labels, CSS stripping, duplicate removal, strict date parsing.
  - **Value:** Transforming inbox clutter into filtered opportunities and deadline alerts.
- End-to-end data flow ASCII diagram.
- Technology mapping table (HDFS, MapReduce, Spark, Hive, Streamlit).

---

## 5. How to Launch the Dashboard

### Using the Runner Script
```bash
bash dashboard/run_dashboard.sh 8501
```

### Using Streamlit Directly
```bash
python3 -m streamlit run dashboard/app.py --server.port=8501
```

Access the dashboard in your web browser at:  
👉 **`http://localhost:8501`**

---

## 6. Viva-Defensibility Cheatsheet

### Q: Why use Streamlit instead of React/Angular or Flask/Django?
- **BDA Focus:** This is a Big Data Analytics project, not a web development project. Streamlit provides rich data visualization components in pure Python, allowing 100% of engineering effort to focus on HDFS, MapReduce, Spark, and Hive.
- **Native Pandas/Plotly Integration:** Zero boilerplate required to convert Hive Parquet query results into interactive charts and filterable tables.

### Q: Why read Parquet/CSV result files instead of querying Hive live?
- **Decoupled Serving Layer (Lambda Architecture):** In production Big Data systems, the analytics engine (Hive) precomputes batch aggregates into a fast serving layer. Live JDBC queries against a HiveServer2 cluster incur 2–5 seconds of JVM/YARN execution overhead per user interaction. Reading pre-materialized Parquet files enables sub-second UI responsiveness.
- **Reliability During Viva Presentations:** The dashboard runs independently of whether Hadoop cluster daemons are active or undergoing maintenance.
