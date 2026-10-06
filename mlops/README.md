# MLOps Subsystem: Experiment Tracking, Model Registry & Observability

This module provides **Machine Learning Operations (MLOps)** capabilities for the **Intelligent College Email Classification and Opportunity Analytics System**. It bridges distributed **PySpark MLlib** batch training with automated governance, experiment tracking, and real-time serving.

---

## 🏗️ Architecture

```
                                  [Raw Email Data]
                                         |
                                         v
                         +-------------------------------+
                         |   Spark MLlib Feature Pipeline|
                         |  (RegexTokenizer + TF-IDF)    |
                         +-------------------------------+
                                         |
                                         v
                  +----------------------------------------------+
                  |           MLOps Training & Tracking          |
                  |             (mlops/tracker.py)               |
                  +----------------------------------------------+
                         |                               |
                         v                               v
           [Run 1: Logistic Regression]       [Run 2: Naive Bayes]
           Accuracy: 90.49% | F1: 90.25%      Accuracy: 86.84% | F1: 86.72%
                         |                               |
                         +---------------+---------------+
                                         |
                                         v
                        +----------------------------------+
                        | Automated Quality Gate (F1 >= 85%)|
                        +----------------------------------+
                                         |
                                         v
                        +----------------------------------+
                        |      MLflow Model Registry       |
                        |      (CollegeEmailClassifier)    |
                        |   v1: Production  |  v2: Staging |
                        +----------------------------------+
                                         |
                         +---------------+---------------+
                         |                               |
                         v                               v
             [Streamlit MLOps Tab]           [Live Inference API]
         - Experiment Comparison Table   - Single Email Classifier
         - Hyperparameters & Drift       - Opportunity Entity Extractor
```

---

## 📁 File Structure

| File | Purpose |
|---|---|
| [`tracker.py`](file:///home/student/Aditya%20BDA%20Project/mlops/tracker.py) | MLflow tracking setup, parameter/metric logging, Spark model serialization, and Model Registry management. |
| [`inference.py`](file:///home/student/Aditya%20BDA%20Project/mlops/inference.py) | Real-time hybrid inference engine (rules + ML) and opportunity entity extraction for the dashboard. |
| [`drift.py`](file:///home/student/Aditya%20BDA%20Project/mlops/drift.py) | Concept drift & data drift engine monitoring category shifts across college terms and confidence health. |
| [`run_experiments.sh`](file:///home/student/Aditya%20BDA%20Project/mlops/run_experiments.sh) | Automated bash runner executing comparative Spark MLlib training runs and logging to MLflow. |
| [`run_mlflow_ui.sh`](file:///home/student/Aditya%20BDA%20Project/mlops/run_mlflow_ui.sh) | Runner to launch the native MLflow tracking server UI. |

---

## 🚀 How to Run

### 1. Run Automated Experiment Suite
Executes both Logistic Regression and Naive Bayes pipelines on Spark and logs results into MLflow:
```bash
bash mlops/run_experiments.sh
```

### 2. Launch MLflow Tracking UI
Starts the MLflow web interface on port 5000:
```bash
bash mlops/run_mlflow_ui.sh 5000
# Access via browser: http://localhost:5000
```

### 3. Run Unit Tests
Verifies tracking retrieval, Model Registry inspection, live inference, and drift calculations:
```bash
python3 -m unittest tests/test_mlops.py
```

### 4. Interactive Streamlit Dashboard
The Streamlit dashboard includes a dedicated **Tab 6: 🤖 MLOps & Model Operations**:
```bash
bash dashboard/run_dashboard.sh 8501
# Access via browser: http://localhost:8501
```

---

## 📊 Tracked Experiment Results

| Run Name | Model | RegParam | MaxIter | Test Accuracy | Test F1 | Stage | Production Gate |
|---|---|---|---|---|---|---|---|
| `spark_mllib_LR` | Logistic Regression | 0.01 | 25 | **90.49%** | **90.25%** | **Production (v1)** | **PASSED ✅** |
| `spark_mllib_NB` | Naive Bayes | 1.0 (smooth) | - | **86.84%** | **86.72%** | **Staging (v2)** | **PASSED ✅** |

---

## 🛡️ Key MLOps Concepts Demonstrated

1. **Experiment Tracking:** Logging hyperparameters, evaluation metrics, and Spark model artifacts with zero external cloud dependencies using a local SQLite backend.
2. **Model Registry & Governance:** Version control for models with explicit stage tagging (`Production`, `Staging`) and automated quality gates requiring test $F_1 \ge 85.0\%$.
3. **Decoupled Real-Time Inference:** Fast, sub-second single email scoring and entity extraction without requiring live Spark cluster invocations.
4. **Drift Observability:** Monitoring distribution shifts across academic seasons (placements vs exams) and tracking prediction confidence stability over 38 months of student emails.
