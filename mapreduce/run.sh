#!/bin/bash
# Run script for the Phase 3 Java MapReduce job on the real dataset.
#
# Removes any pre-existing output directory (MapReduce refuses to write into
# an existing output path), then submits the job to YARN.
#
# Usage: bash mapreduce/run.sh

set -euo pipefail

export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_PATH="$PROJECT_ROOT/mapreduce/domain-month-count.jar"

INPUT="/user/student/email_project/raw/structured/emails.jsonl"
OUTPUT="/user/student/email_project/results/domain_month_counts"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: $JAR_PATH not found. Run 'bash mapreduce/build.sh' first." >&2
    exit 1
fi

echo "== Removing any existing output directory on HDFS =="
hdfs dfs -rm -r -f "$OUTPUT"

echo "== Submitting MapReduce job to YARN =="
# Rely on manifest's Main-Class rather than passing class name explicitly.
# Hadoop 3.4.x with manifest Main-Class interprets explicit class names
# differently, leading to arg-count mismatches -- omit for simplicity.
hadoop jar "$JAR_PATH" "$INPUT" "$OUTPUT"

echo "== Job finished. Output listing: =="
hdfs dfs -ls "$OUTPUT"
