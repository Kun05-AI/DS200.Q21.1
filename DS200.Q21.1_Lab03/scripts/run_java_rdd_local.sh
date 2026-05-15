#!/usr/bin/env bash
# file: scripts/run_java_rdd_local.sh
# Run Task1..Task6 sequentially (local mode)
set -euo pipefail

if [ -z "${SPARK_HOME:-}" ]; then
  echo "ERROR: SPARK_HOME is not set. Export SPARK_HOME to your Spark installation (e.g. E:/spark-4.1.1-bin-hadoop3)."
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/spark/java/lab03-rdd/target/spark-rdd-lab03-1.0-SNAPSHOT.jar"
DATA="$ROOT/data"
OUT="$ROOT/output"

mkdir -p "$OUT"

run() {
  echo
  echo "==> Running: $1"
  "$SPARK_HOME/bin/spark-submit" --master local[*] --class "$1" "$JAR" "${@:2}"
  echo "==> Finished: $1"
}

# Task1
run ds200.lab03.task1.Task1App \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" "$OUT/task1_movie_ratings.txt"

# Task2
run ds200.lab03.task2.Task2App \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" "$OUT/task2_genre_ratings.txt"

# Task3 (topN = 20)
run ds200.lab03.task3.Task3App \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" "$OUT/task3_top_movies.txt" 20

# Task4 (minRatings = 5)
run ds200.lab03.task4.Task4App \
  "$DATA/users.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" "$OUT/task4_user_summary.txt" 5

# Task5
run ds200.lab03.task5.Task5App \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" "$OUT/task5_rating_dist.txt"

# Task6
run ds200.lab03.task6.Task6App \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" "$OUT/task6_genre_stats.txt"

echo
echo "All tasks finished. Outputs are in $OUT"
