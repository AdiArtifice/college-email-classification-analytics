# Phase 3 — Java Hadoop MapReduce Job

**Intelligent College Email Classification and Opportunity Analytics System**

---

## Purpose

Demonstrates a **classic distributed batch-processing task** using Java Hadoop MapReduce (implementation_plan.md, Phase 3) as a concrete example of **Veracity** (handling incomplete/malformed records gracefully without job failure) and **Volume** (distributed aggregation across 7,276 email records).

---

## Job Specification

**Task:** Aggregate email counts per `sender_domain` per month (`yyyy-MM`).

**Input:**  
`/user/student/email_project/raw/structured/emails.jsonl` on HDFS (33.7 MB JSONL, one JSON object per line, UTF-8).

**Output:**  
`/user/student/email_project/results/domain_month_counts/` on HDFS — tab-separated rows:  
```
<sender_domain>\t<yyyy-MM>\t<count>
```

---

## Implementation

### Mapper (`DomainMonthCountMapper.java`)

1. Reads each line as plain text via `TextInputFormat` (no Parquet/Avro JARs needed).
2. Parses line as JSON using Jackson (already bundled in Hadoop 3.4.1 classpath).
3. Extracts `sender_domain` and `date_utc` fields (ISO-8601 UTC format from parser).
4. Emits composite key: `domain\tyyyy-MM`, value: `1`.
5. **Veracity handling:** Malformed JSON, missing `sender_domain`, or missing `date_utc` → skip record and increment Hadoop Counters (`MALFORMED_JSON`, `MISSING_DOMAIN`, `MISSING_DATE`, `EMITTED`) rather than failing the job.

### Reducer (`DomainMonthCountReducer.java`)

- Sums counts per key.
- **Optimization:** Reused as the job's **Combiner** (associative sum reduces shuffle I/O).

### Driver (`DomainMonthCountDriver.java`)

- Configures job: sets Mapper, Combiner, Reducer, input/output paths.
- Sets `TextInputFormat` explicitly (per implementation_plan: "no Parquet JARs needed").
- Waits for completion; exits with 0 on success, 1 on failure.

---

## Build & Run

### Build

Compiles with **Java 11** explicitly (bypassing the system's default `javac` alternative which resolves to Java 21 — see environment audit):

```bash
bash mapreduce/build.sh
```

**Output:** `mapreduce/domain-month-count.jar` (manifest `Main-Class` set to `DomainMonthCountDriver`).

### Run

```bash
bash mapreduce/run.sh
```

**Internal steps:**
1. Removes any existing output directory (`hdfs dfs -rm -r -f`)
2. Submits job to YARN: `hadoop jar domain-month-count.jar <input> <output>`
3. Lists output on completion

---

## Validation (verified)

**Cross-checked** MapReduce aggregated totals against Phase 1 Python-computed domain counts:

| Domain | Expected (Phase 1) | MapReduce Actual | Status |
|---|---|---|---|
| `dare2compete.news` | 934 | 934 | ✅ |
| `sjcem.edu.in` | 908 | 908 | ✅ |
| `classroom.google.com` | 454 | 454 | ✅ |
| `eduskillsfoundation.org` | 219 | 219 | ✅ |
| `naukri.com` | 162 | 162 | ✅ |

**Totals:**  
- Unique domains: 183  
- Domain-month pairs: 1,113  
- Grand total emails: **7,276** (matches parser output exactly)

---

## Counters (final job report — example)

```
RecordCounter
    EMITTED=7276
    MALFORMED_JSON=0
    MISSING_DATE=0
    MISSING_DOMAIN=0
```

Zero errors — all records from Phase 1's parser were structurally complete (no missing `sender_domain`/`date_utc` fields, no malformed JSON lines). The counter infrastructure is present to handle real-world data quality issues defensively.

---

## Viva Talking Points

1. **Why MapReduce for this project?**  
   Demonstrates classic distributed batch processing (course requirement); proves data can be processed in parallel across cluster nodes (scalability / Volume).

2. **Why not just use Spark for this aggregation?**  
   MapReduce is the *canonical* Hadoop batch framework and aligns with BDA coursework. Spark is used for more complex NLP/ML tasks in Phase 4 (TF-IDF, classification) where in-memory iterative processing is valuable.

3. **Input format — why TextInputFormat / JSONL instead of Parquet?**  
   JSONL via TextInputFormat requires zero external JARs (no Parquet/Avro dependencies), keeping the MapReduce job dependency-free. The Parquet output from Phase 1 feeds Spark (Phase 4), not MapReduce.

4. **Combiner optimization?**  
   `DomainMonthCountReducer` is reused as Combiner (associative sum). Reduces shuffle traffic by pre-aggregating counts on each Mapper node before sending to Reducers — a best practice for count-style aggregations.

5. **Veracity handling?**  
   Malformed/incomplete records are skipped with Hadoop Counters tracking the skip reasons (`MALFORMED_JSON`, `MISSING_DOMAIN`, `MISSING_DATE`). This prevents job failure due to data-quality issues while maintaining observability.

6. **Why the unusual package structure (`com.bda.email` instead of `com.bda.email.mapreduce`)?**  
   Workaround for a path-resolution quirk in the development tooling when a directory name (`mapreduce`) repeated in the nested package path. Functionally identical; clarified in comments.

---

## Outputs on HDFS

```
hdfs:///user/student/email_project/results/domain_month_counts/
├── _SUCCESS                (0 bytes — completion flag)
└── part-r-00000            (29 KB — aggregated counts)
```

Sample output rows:

```
dare2compete.news	2024-06	14
dare2compete.news	2024-07	44
dare2compete.news	2024-08	57
...
sjcem.edu.in	2025-03	121
sjcem.edu.in	2025-04	134
...
```

---

**Phase 3 complete. Ready for Phase 4 (Spark NLP/ML pipeline).**
