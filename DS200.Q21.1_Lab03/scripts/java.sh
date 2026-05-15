#!/usr/bin/env bash
# file: scripts/java.sh
# Usage: ./java.sh <MainClass> [args...]
set -euo pipefail

if [ -z "${SPARK_HOME:-}" ]; then
  echo "ERROR: SPARK_HOME is not set. Export SPARK_HOME to your Spark installation (e.g. E:/spark-4.1.1-bin-hadoop3)."
  exit 2
fi

if [ $# -lt 1 ]; then
  echo "Usage: $0 <MainClass> [args...]"
  exit 1
fi

MAIN_CLASS="$1"
shift

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/spark/java/lab03-rdd/target/spark-rdd-lab03-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Jar not found at $JAR. Build first: mvn -f spark/java/lab03-rdd clean package -DskipTests"
  exit 3
fi

"$SPARK_HOME/bin/spark-submit" \
  --master local[*] \
  --class "$MAIN_CLASS" \
  "$JAR" "$@"
