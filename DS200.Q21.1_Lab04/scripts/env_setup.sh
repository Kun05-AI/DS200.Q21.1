#!/usr/bin/env bash
set -euo pipefail

# Lab 04 environment bootstrap for Git Bash / Linux-like shells.
# Adjust paths if your Java installation differs.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export JAVA_HOME="${JAVA_HOME:-/d/UIT/Java/jdk-17.0.19+10}"
export MAVEN_HOME="${MAVEN_HOME:-/d/UIT/Java/apache-maven-3.9.15}"
export SPARK_HOME="${SPARK_HOME:-/d/UIT/Java/spark-4.1.1-bin-hadoop3}"

export PATH="$JAVA_HOME/bin:$MAVEN_HOME/bin:$SPARK_HOME/bin:$PATH"

export LAB04_ROOT="$PROJECT_ROOT"
export LAB04_DATA_DIR="$PROJECT_ROOT/data"
export LAB04_OUTPUT_DIR="$PROJECT_ROOT/output"
export LAB04_SCRIPTS_DIR="$PROJECT_ROOT/scripts"
export LAB04_SPARK_DIR="$PROJECT_ROOT/spark/java/lab04"

mkdir -p "$LAB04_OUTPUT_DIR"
