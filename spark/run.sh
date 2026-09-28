#!/usr/bin/env bash
# ==============================================================================
# Intelligent College Email Classification — Spark Pipeline Runner (Phase 4)
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/opt/spark
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PROJECT_ROOT:$PYTHONPATH"

echo "======================================================================"
echo " Starting Spark Classification Pipeline"
echo "======================================================================"
echo "JAVA_HOME:    $JAVA_HOME"
echo "SPARK_HOME:   $SPARK_HOME"
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "======================================================================"

# Ensure HDFS output directory exists / is cleaned if needed
HDFS_OUTPUT="hdfs://localhost:9000/user/student/email_project/features/emails.parquet"

"$SPARK_HOME/bin/spark-submit" \
  --master local[2] \
  --driver-memory 2G \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.ui.enabled=false \
  "$SCRIPT_DIR/pipeline.py" "$@"

echo "======================================================================"
echo " Spark Pipeline Run Completed Successfully!"
echo "======================================================================"
