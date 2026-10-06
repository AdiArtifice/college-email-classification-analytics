#!/usr/bin/env bash
# ==============================================================================
# MLOps Experiment Runner: Automated Model Training & Comparative Tracking
# ==============================================================================
# Runs two competing Spark MLlib models (Logistic Regression & Naive Bayes),
# logs their hyperparameters, metrics, and models to MLflow, and evaluates
# them against the Production quality gate (F1 >= 0.85).
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/opt/spark
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PROJECT_ROOT:$PYTHONPATH"
export MLFLOW_DISABLE_AGENT_HINT=1

echo "======================================================================"
echo " Starting MLOps Experiment Suite (PySpark MLlib + MLflow)"
echo "======================================================================"
echo "Project Root: $PROJECT_ROOT"
echo "Experiment:   College_Email_Classification"
echo "Registry:     CollegeEmailClassifier"
echo "======================================================================"

# Experiment 1: Logistic Regression
echo ""
echo ">>> [EXPERIMENT 1/2] Training & Evaluating Multinomial Logistic Regression..."
"$SPARK_HOME/bin/spark-submit" \
  --master local[2] \
  --driver-memory 2G \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.ui.enabled=false \
  "$PROJECT_ROOT/spark/pipeline.py" \
  --model-type lr \
  --eval \
  --register-model \
  --min-f1 0.85

# Experiment 2: Naive Bayes
echo ""
echo ">>> [EXPERIMENT 2/2] Training & Evaluating Multinomial Naive Bayes..."
"$SPARK_HOME/bin/spark-submit" \
  --master local[2] \
  --driver-memory 2G \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.ui.enabled=false \
  "$PROJECT_ROOT/spark/pipeline.py" \
  --model-type nb \
  --eval \
  --register-model \
  --min-f1 0.85

echo ""
echo "======================================================================"
echo " MLOps Experiment Suite Finished Successfully!"
echo " All runs, metrics, and models tracked in MLflow (mlflow.db)."
echo "======================================================================"
