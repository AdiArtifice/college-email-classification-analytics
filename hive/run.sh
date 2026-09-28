#!/usr/bin/env bash
# ==============================================================================
# Intelligent College Email Classification — Hive Analytics Runner (Phase 6)
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/opt/spark
export HIVE_HOME=/usr/local/hive
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PROJECT_ROOT:$PYTHONPATH"

echo "======================================================================"
echo " Executing Hive Analytics Suite"
echo "======================================================================"
echo "JAVA_HOME:    $JAVA_HOME"
echo "HIVE_HOME:    $HIVE_HOME"
echo "SPARK_HOME:   $SPARK_HOME"
echo "PROJECT_ROOT: $PROJECT_ROOT"
echo "======================================================================"

python3 "$SCRIPT_DIR/run_analytics.py" "$@"

echo "======================================================================"
echo " Hive Analytics Run Completed Successfully!"
echo "======================================================================"
