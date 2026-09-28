#!/bin/bash
# Build script for the Phase 3 Java MapReduce job.
#
# Compiles explicitly against Java 11 (the JVM Hadoop 3.4.1 runs on in this
# environment). The system's default `javac` alternative resolves to
# Java 21 (see environment audit) which would silently produce Java-21
# bytecode incompatible with expectations -- so we bypass PATH resolution
# entirely and invoke the Java 11 javac directly.
#
# Usage: bash mapreduce/build.sh

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MR_DIR="$PROJECT_ROOT/mapreduce"
SRC_DIR="$MR_DIR/src"
BUILD_DIR="$MR_DIR/build"
JAR_PATH="$MR_DIR/domain-month-count.jar"

JAVA11_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
JAVAC="$JAVA11_HOME/bin/javac"
JAR_TOOL="$JAVA11_HOME/bin/jar"

if [ ! -x "$JAVAC" ]; then
    echo "ERROR: Java 11 javac not found at $JAVAC" >&2
    exit 1
fi

echo "== Cleaning previous build =="
rm -rf "$BUILD_DIR" "$JAR_PATH"
mkdir -p "$BUILD_DIR"

echo "== Resolving Hadoop classpath =="
HADOOP_CP="$(hadoop classpath)"

echo "== Compiling (Java 11, --release 11) =="
"$JAVAC" --release 11 -cp "$HADOOP_CP" -d "$BUILD_DIR" \
    "$SRC_DIR"/com/bda/email/*.java

echo "== Packaging jar =="
"$JAR_TOOL" cfe "$JAR_PATH" com.bda.email.DomainMonthCountDriver \
    -C "$BUILD_DIR" .

echo "== Build complete: $JAR_PATH =="
"$JAR_TOOL" tf "$JAR_PATH"
