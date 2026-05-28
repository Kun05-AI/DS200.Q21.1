#!/usr/bin/env bash
# DS200 Lab 04 - Personalized Screenshot Helper
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

STEP="${1:-all}"

STUDENT_NAME="${STUDENT_NAME:-Vũ Việt Cương}"
STUDENT_ID="${STUDENT_ID:-23520213}"

header() {
  local step="$1"
  local title="$2"

  echo "╔══════════════════════════════════════════════════════╗"
  echo "║                DS200.Q21.1 - Lab 04                  ║"
  echo "╠══════════════════════════════════════════════════════╣"
  printf "║ %-52s ║\n" "Step    : ${step}"
  printf "║ %-52s ║\n" "Task    : ${title}"
  printf "║ %-57s ║\n" "Student : ${STUDENT_NAME}"
  printf "║ %-52s ║\n" "ID      : ${STUDENT_ID}"
  printf "║ %-52s ║\n" "User    : $(whoami)"
  printf "║ %-52s ║\n" "Branch  : $(git branch --show-current 2>/dev/null || echo unknown)"
  printf "║ %-52s ║\n" "Time    : $(date)"
  echo "╚══════════════════════════════════════════════════════╝"
  echo ""
}

pause() {
  if [[ "$STEP" == "all" ]]; then
    echo ""
    echo "[INFO] Capture screenshot if needed."
    echo "[INFO] Press ENTER to continue..."
    read -r < /dev/tty
  fi
}

show_file() {
  local step="$1"
  local title="$2"
  local file="$3"

  header "$step" "$title"

  if [[ -f "$file" ]]; then
    echo "──────────────────────────────────────────────────────────────────────"
    echo "[STATUS] OUTPUT FILE FOUND"
    echo "[FILE]   $file"
    echo "[LINES]  $(wc -l < "$file")"
    echo "──────────────────────────────────────────────────────────────────────"
    echo ""
    cat "$file"
    echo ""
    echo "──────────────────────────────────────────────────────────────────────"
    echo "[END OF OUTPUT]"
    echo "──────────────────────────────────────────────────────────────────────"
  else
    echo "──────────────────────────────────────────────────────────────────────"
    echo "[ERROR] Output file not found"
    echo "[PATH ] $file"
    echo "──────────────────────────────────────────────────────────────────────"
  fi

  pause
}

if [[ "$STEP" == "all" || "$STEP" == "1" ]]; then
  header "1" "Environment Information"

  echo "[SYSTEM INFORMATION]"
  echo "User          : $(whoami)"
  echo "Hostname      : $(hostname)"
  echo "Current Path  : $(pwd)"
  echo "Date          : $(date)"
  echo ""

  echo "[JAVA VERSION]"
  java -version 2>&1 || echo "(java not found)"
  echo ""

  echo "[MAVEN VERSION]"
  mvn -version 2>&1 | head -5 || echo "(maven not found)"
  echo ""

  echo "[SPARK VERSION]"
  spark-submit --version 2>&1 | head -5 || echo "(spark-submit not found)"
  echo ""

  echo "──────────────────────────────────────────────────────────────────────"
  echo "[ENVIRONMENT CHECK COMPLETED]"
  echo "──────────────────────────────────────────────────────────────────────"

  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "2" ]]; then
  show_file "2" "Task 1 - Load CSV files with inferSchema" "output/task1_dataset_initializer.txt"
fi
if [[ "$STEP" == "all" || "$STEP" == "3" ]]; then
  show_file "3" "Task 2 - Total orders, customers, sellers" "output/task2_general_statistics.txt"
fi
if [[ "$STEP" == "all" || "$STEP" == "4" ]]; then
  show_file "4" "Task 3 - Orders by country (descending)" "output/task3_orders_by_country.txt"
fi
if [[ "$STEP" == "all" || "$STEP" == "5" ]]; then
  show_file "5" "Task 4 - Orders by year and month" "output/task4_orders_by_year_month.txt"
fi
if [[ "$STEP" == "all" || "$STEP" == "6" ]]; then
  show_file "6" "Task 5 - Review score statistics" "output/task5_review_distribution.txt"
fi
if [[ "$STEP" == "all" || "$STEP" == "8" ]]; then
  show_file "7" "Task 7 - Top-selling products and avg review score" "output/task7_top_selling_products.txt"
fi
if [[ "$STEP" == "all" || "$STEP" == "9" ]]; then
  show_file "8" "Task 8 - Delivery performance" "output/task8_delivery_performance.txt"
fi
if [[ "$STEP" == "all" || "$STEP" == "10" ]]; then
  show_file "9" "Task 9 - Customer segmentation" "output/task9_customer_segmentation.txt"
