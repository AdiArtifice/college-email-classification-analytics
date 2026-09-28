# Phase 4 — Apache Spark MLlib Pipeline

**Intelligent College Email Classification and Opportunity Analytics System**  
**Component:** `spark/`  
**Engine:** Apache Spark 3.5.6 (PySpark) on OpenJDK 11 & Python 3.12  

---

## 1. Overview & Objectives

Phase 4 implements the distributed processing, NLP feature extraction, weak supervision labeling, and machine learning classification pipeline using **Apache Spark (PySpark)** and **Spark MLlib**.

### Core Objectives:
1. **Load Cleaned Parquet:** Read structurally cleaned and deduplicated records (`7,276` unique emails) from HDFS (`/user/student/email_project/cleaned/emails.parquet`).
2. **Text Preprocessing:** Lowercase, remove CSS style artifacts from HTML fallback, strip quoted thread replies (`> ...`), strip forwarded message headers, drop URLs/emails/signatures, and perform spell-light normalization (character repetition reduction).
3. **TF-IDF Feature Extraction:** Extract distributed term features via Spark MLlib `RegexTokenizer`, `StopWordsRemover`, `HashingTF` (20,000 features), and `IDF`.
4. **Weak Supervision Labeling:** Bootstrap training labels from Gmail system categories (`Category promotions` → Newsletter/Promotional, `Category purchases`/`bills` → Administrative, `Category social` → Miscellaneous), handle noisy signals (`Category forums` mapped to Workshop/Event only when accompanied by explicit keywords), and enforce high-confidence domain + keyword rules for opportunities (Unstop, Naukri, EduSkills, Google Classroom, SJCEM).
5. **Classifier Training & Evaluation:** Train and evaluate multinomial **Logistic Regression** and **Naive Bayes** models on an 80/20 train/test split.
6. **Hybrid Decision Resolution:** Assign deterministic high-confidence labels with 100% confidence (`prediction_method = 'rule'`), and apply the trained ML model for the remaining unassigned emails (`prediction_method = 'ml'`) with extracted maximum class probability as confidence.
7. **Opportunity Metadata Enrichment:** Extract `campus_type`, `organization`, `opportunity_type`, `deadline`, `event_date`, and `keywords`.
8. **Persist Enriched Features:** Save the final enriched dataset to HDFS (`/user/student/email_project/features/emails.parquet`) and local disk.

---

## 2. Architecture & Data Flow

```
+-------------------------------------------------------------------------+
|                  HDFS: cleaned/emails.parquet (7,276)                   |
+-------------------------------------------------------------------------+
                                    |
                                    v
             [1] Text Preprocessing & Cleaning (spark/preprocess.py)
                 - Strip CSS rules & inline styles from html2text
                 - Strip quoted lines (> ...) and forwarded headers
                 - Strip URLs, emails, signatures, markdown artifacts
                 - Subject boosting (weight subject tokens higher)
                 - Spell-light normalization (reduce repeated chars)
                                    |
                                    v
            [2] Weak Supervision Labeling (spark/labeling.py)
                 - Priority 1: Opportunity domain + keyword rules
                 - Priority 2: College & classroom rules (Academic, Exam, Admin)
                 - Priority 3: Noisy signal filter (forums + event keywords)
                 - Priority 4: Gmail Category bootstrap
                 - Result: 5,259 Labeled (72.3%) | 2,017 Unlabeled (27.7%)
                                    |
                                    v
            [3] TF-IDF Feature Extraction (spark/classifier.py)
                 RegexTokenizer -> StopWordsRemover -> HashingTF(20k) -> IDF
                                    |
                                    v
            [4] MLlib Classification (Logistic Regression / Naive Bayes)
                 - 80/20 train/test split evaluation (Accuracy: 90.49%, F1: 90.25%)
                 - Model trained on full labeled dataset
                 - Inference on all records to compute class & max probability
                                    |
                                    v
            [5] Hybrid Decision & Confidence Resolution
                 - If rule matched: category = rule_label, confidence = 1.0, method = 'rule'
                 - If rule is null: category = ml_category, confidence = ml_prob, method = 'ml'
                                    |
                                    v
            [6] Opportunity Information Extraction (spark/extract.py)
                 - campus_type: On-campus vs Off-campus
                 - organization: Unstop, Naukri, EduSkills, SJCEM, etc.
                 - opportunity_type: AI/ML, Data Science, Web Dev, etc.
                 - deadline / event_date: ISO-8601 YYYY-MM-DD or null
                 - keywords: technology & domain skill tags
                                    |
                                    v
+-------------------------------------------------------------------------+
|                 HDFS: features/emails.parquet (7,276)                   |
|                 (Parquet schema ready for Hive & Streamlit)             |
+-------------------------------------------------------------------------+
```

---

## 3. Directory Layout & Module Responsibilities

```
spark/
├── __init__.py           # Spark module definition
├── preprocess.py         # Regex text cleaning, CSS removal, normalization UDF
├── labeling.py           # Weak supervision rules, priority hierarchy, UDF
├── classifier.py         # TF-IDF pipeline, LR & NB estimators, evaluation, hybrid logic
├── extract.py            # Opportunity metadata extraction (deadline, campus, org)
├── pipeline.py           # Main batch pipeline driver
├── run.sh                # Production runner script with environment bindings
└── README.md             # Architecture, validation, and viva documentation
```

### Module Roles:
- **`preprocess.py`:** Handles data **Veracity**. Strips CSS fragments (from the 2,004 HTML fallback records), removes quoted replies and signatures that distort term frequencies, and applies subject boosting.
- **`labeling.py`:** Implements rule-based weak supervision. Enforces the prompt requirement that `forums` is noisy and only mapped when combined with explicit event keywords.
- **`classifier.py`:** Manages Spark MLlib pipeline. Strips internal vector UDTs prior to output to ensure standard Parquet serialization.
- **`extract.py`:** Extracts structured opportunity attributes without guessing (deadlines left null if ambiguous).
- **`pipeline.py`:** Coordinates the end-to-end execution, logs metrics, checks row count invariants, and emits a JSON run summary.

---

## 4. Benchmark: Model Evaluation

Evaluated on an 80/20 train/test split of the weakly supervised records (4,207 training, 1,052 testing):

| Classifier | Test Accuracy | Weighted F1 | Weighted Precision | Weighted Recall |
|---|---|---|---|---|
| **Multinomial Logistic Regression (LR)** | **90.49%** | **90.25%** | **90.55%** | **90.49%** |
| **Multinomial Naive Bayes (NB)** | **86.83%** | **87.19%** | **88.10%** | **86.83%** |

*Logistic Regression was selected as the default classifier due to higher accuracy, stronger precision across minor opportunity classes, and well-calibrated class probability vectors.*

---

## 5. Dataset Results & Final Distributions

From a total of **7,276 unique emails**:

### Prediction Method Breakdown:
- **Rule-assigned (High confidence):** `5,259` records (**72.3%**)
- **ML-predicted (TF-IDF inference):** `2,017` records (**27.7%**)

### Category Distribution:

| Category | Count | Percentage | Primary Source |
|---|---|---|---|
| **Newsletter/Promotional** | 2,951 | 40.6% | Gmail Category promotions + ML |
| **Academic** | 1,701 | 23.4% | Google Classroom, college notices + ML |
| **Hackathon/Competition** | 1,069 | 14.7% | Unstop (dare2compete.news), coding challenges |
| **Internship** | 535 | 7.4% | EduSkills, Internshala, internship notices |
| **Interview** | 310 | 4.3% | Interview schedules, call letters, shortlist notices |
| **Job/Placement** | 266 | 3.7% | Naukri, placement cell communications |
| **Miscellaneous** | 180 | 2.5% | Social notifications, accounts, system emails |
| **Workshop/Event** | 126 | 1.7% | Filtered forums + webinars, seminars, bootcamps |
| **Administrative** | 97 | 1.3% | Fees, bonafide, admission, railway concessions |
| **Examination** | 23 | 0.3% | Hall tickets, exam timetables, revaluation notices |
| **Scholarship** | 18 | 0.2% | Financial aid and student scholarship notices |
| **Total** | **7,276** | **100.0%** | **Verified Row Count Integrity ✅** |

---

## 6. Output Schema (`features/emails.parquet`)

The output schema retains all 17 base attributes from `cleaned/`, augmented with derived NLP, ML, and extraction fields (27 total columns):

| Column Name | Type | Description |
|---|---|---|
| `message_id` | `string` | Unique RFC 822 Message-ID (primary key) |
| `thread_id` | `string` | In-Reply-To / References message thread ID |
| `sender` | `string` | Decoded sender display name |
| `sender_email` | `string` | Sender email address |
| `sender_domain` | `string` | Sender domain (e.g., `dare2compete.news`) |
| `recipients` | `array<string>` | List of To/Cc recipients |
| `subject` | `string` | Decoded subject line |
| `date_utc` | `string` | Normalized ISO-8601 UTC timestamp |
| `body_text` | `string` | Plain-text email body (or html2text fallback) |
| `body_length` | `int` | Character length of body |
| `has_attachment` | `boolean` | Attachment indicator flag |
| `attachment_count` | `int` | Total attachment count |
| `gmail_labels` | `array<string>` | Gmail system labels |
| `is_unread` | `boolean` | Unread status flag |
| `is_spam` | `boolean` | Spam status flag |
| `is_sent` | `boolean` | Sent email flag |
| `is_important` | `boolean` | Important label flag |
| `cleaned_text` | `string` | Normalized NLP text (subject boosted, CSS/URLs stripped) |
| `rule_category` | `string` | Weak supervision rule label (`null` if unassigned) |
| `ml_category` | `string` | Category predicted by Spark MLlib model |
| `ml_confidence` | `float` | Maximum predicted class probability |
| **`category`** | `string` | **Final hybrid category** |
| **`confidence`** | `double` | **Confidence score (1.0 for rules, prob for ML)** |
| **`prediction_method`** | `string` | **Method: `'rule'` or `'ml'`** |
| `campus_type` | `string` | `'On-campus'`, `'Off-campus'`, or `null` |
| `organization` | `string` | Extracted organization name (e.g., Unstop, SJCEM) |
| `opportunity_type` | `string` | Sub-domain (AI/ML, Data Science, Web Dev, etc.) |
| `deadline` | `string` | ISO date `YYYY-MM-DD` or `null` |
| `event_date` | `string` | ISO date `YYYY-MM-DD` or `null` |
| `keywords` | `array<string>` | Extracted technology and opportunity skill keywords |

---

## 7. How to Run

### Standard Execution (via runner script)
```bash
bash spark/run.sh
```

### Custom Options (via `spark-submit`)
```bash
/opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 2G \
  spark/pipeline.py \
  --model-type lr \
  --eval \
  --input hdfs://localhost:9000/user/student/email_project/cleaned/emails.parquet \
  --output hdfs://localhost:9000/user/student/email_project/features/emails.parquet \
  --local-output data/output/features/emails.parquet \
  --summary-output data/output/features_summary.json
```

---

## 8. Viva-Defensibility Cheatsheet

### Q: Why use Spark rather than Python/scikit-learn for this phase?
- **Horizontal Scalability:** While 7.3K emails can fit in memory on a single machine, this architecture is designed for an institutional email platform aggregating tens of thousands of student accounts. Spark processes partitions across distributed executors with zero code modifications.
- **In-Memory Caching:** Feature extraction (`HashingTF` + `IDF`) and model training reuse cached DataFrames across iterations without disk re-reads.
- **Native HDFS Integration:** Direct streaming read/write from and to HDFS blocks without intermediate local file staging.

### Q: Why Weak Supervision instead of manual labeling or deep learning?
- **Zero Labeling Overhead:** College students receive thousands of emails; manual annotation of 7,276 records is impractical.
- **Explainability:** Weak supervision rules (domain + keywords + Gmail labels) provide transparent, auditable seeds.
- **Hybrid Coverage:** Rules capture 72.3% of records with high precision; Spark MLlib generalizes to the remaining 27.7% ambiguous emails.

### Q: Why use HashingTF instead of CountVectorizer?
- `HashingTF` utilizes the feature hashing trick (MurmurHash3), mapping words directly into fixed-size feature indices (`numFeatures=20,000`) without maintaining an expensive global vocabulary dictionary across worker nodes. This eliminates cluster-wide shuffle and dictionary broadcast overhead.

### Q: Why drop intermediate vector columns before saving to Parquet?
- Parquet is consumed downstream by **Apache Hive** (Phase 6) and **Streamlit** (Phase 7). Spark MLlib's `VectorUDT` is a specialized internal Java type that causes deserialization errors in standard Hive Parquet SerDe. Flattening the output into primitive and array types ensures 100% interoperability across the Hadoop ecosystem.
