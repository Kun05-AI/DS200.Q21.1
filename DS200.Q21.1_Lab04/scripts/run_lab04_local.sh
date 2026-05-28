#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/env_setup.sh"

cd "$PROJECT_ROOT/spark/java/lab04"

mvn clean package -DskipTests

JAR="$PROJECT_ROOT/spark/java/lab04/target/lab04-1.0.0.jar"
DATA="$PROJECT_ROOT/data"
OUT="$PROJECT_ROOT/output"

spark_submit() {
  "$SPARK_HOME/bin/spark-submit" --master local[*] --class "$1" "$JAR" "${@:2}"
}

spark_submit ds200.lab04.task1.Task1_DatasetInitializer \
  "$DATA/Orders.csv" \
  "$DATA/Customer_List.csv" \
  "$DATA/Order_Items.csv" \
  "$DATA/Order_Reviews.csv" \
  "$DATA/Products.csv" \
  "$OUT/task1_dataset_initializer.txt"

spark_submit ds200.lab04.task2.Task2_GeneralStatistics \
  "$DATA/Orders.csv" \
  "$DATA/Customer_List.csv" \
  "$DATA/Order_Items.csv" \
  "$DATA/Order_Reviews.csv" \
  "$DATA/Products.csv" \
  "$OUT/task2_general_statistics.txt"

spark_submit ds200.lab04.task3.Task3_OrdersByCountry \
  "$DATA/Orders.csv" \
  "$DATA/Customer_List.csv" \
  "$OUT/task3_orders_by_country.txt"

spark_submit ds200.lab04.task4.Task4_OrdersByYearMonth \
  "$DATA/Orders.csv" \
  "$OUT/task4_orders_by_year_month.txt"

spark_submit ds200.lab04.task5.Task5_ReviewDistribution \
  "$DATA/Order_Reviews.csv" \
  "$OUT/task5_review_distribution.txt"

spark_submit ds200.lab04.task7.Task7_TopSellingProductsAndAvgReview \
  "$DATA/Order_Items.csv" \
  "$DATA/Order_Reviews.csv" \
  "$DATA/Products.csv" \
  "$OUT/task7_top_selling_products_and_avg_review.txt"

spark_submit ds200.lab04.task8.Task8_DeliveryPerformance \
  "$DATA/Orders.csv" \
  "$DATA/Order_Items.csv" \
  "$OUT/task8_delivery_performance.txt"

spark_submit ds200.lab04.task9.Task9_CustomerSegmentation \
  "$DATA/Orders.csv" \
  "$DATA/Customer_List.csv" \
  "$DATA/Order_Items.csv" \
  "$OUT/task9_customer_segmentation.txt"

echo "All Lab 04 tasks completed."