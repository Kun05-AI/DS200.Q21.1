<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 04)</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Java-17%2B-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 17+" />
  <img src="https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark 4.1.1" />
  <img src="https://img.shields.io/badge/DataFrame%2FDataset-Java-blue?style=for-the-badge" alt="Java DataFrame/Dataset" />
  <img src="https://img.shields.io/badge/Platform-Windows%2011-0078D4?style=for-the-badge&logo=windows&logoColor=white" alt="Windows 11" />
</p>

<p align="center">
  <a href="https://github.com/Kun05_AI"><img src="https://img.shields.io/badge/GitHub-Kun05__AI-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/"><img src="https://img.shields.io/badge/LinkedIn-Vu%20Viet%20Cuong-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23520213@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23520213%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab04/`  
**Repository:** `DS200.Q21.1_Lab04`  
**Course:** Big Data Analysis (DS200.Q21)  
**Teaching assistant:** Nguyễn Hiếu Nghĩa  
**Lecturer:** Hà Minh Tân

---

## Student information

| Student ID | Full name | GitHub | Email | LinkedIn |
|:--:|---|---|---|---|
| 23520213 | Vũ Việt Cương | [Kun05_AI](https://github.com/Kun05_AI) | 23520213@gm.uit.edu.vn | [Profile](https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/) |

---

## 1. Overview

This laboratory assignment focuses on the application of Apache Spark for batch data processing using the DataFrame/Dataset API in Java.  
The project reads multiple CSV files, performs data cleaning and transformation, computes summary statistics, and writes the final results into text files under the `output/` directory.

The main purpose of this lab is to:
- practice building scalable data pipelines with Spark;
- understand how to organize transformation logic in a clear and reproducible manner;
- produce deterministic outputs that can be verified against expected results.

---

## 2. Technologies Used

| Component | Version / Note |
|---|---|
| Programming language | Java 17 |
| Big data framework | Apache Spark 4.1.1 |
| Processing model | Spark DataFrame / Dataset |
| Build tool | Maven |
| Operating system | Windows 11 |

---

## 3. Project Structure

```text
DS200.Q21.1_Lab04/
├── data/
│   ├── orders.csv
│   ├── customer_list.csv
│   ├── order_items.csv
│   ├── order_reviews.csv
│   ├── products.csv
│   └── sellers.csv
├── output/
│   ├── task1_dataset_initializer.txt
│   ├── task2_general_statistics.txt
│   ├── task3_orders_by_country.txt
│   ├── task4_orders_by_year_month.txt
│   ├── task5_review_distribution.txt
│   ├── task7_top_selling_products.txt
│   ├── task8_delivery_performance.txt
│   ├── task9_customer_segmentation.txt
├── screenshots/
├── scripts/
│   ├── env_setup.sh
│   ├── run_lab04_local.sh
│   └── screenshot_lab04.sh
└── spark/
    └── java/
        └── lab04/
            ├── pom.xml
            └── target/lab04-1.0.0.jar
            └── src/java/src/main/java/ds200/lab04/Task...
```

---

## 4. Input Data

All input files are in CSV format, encoded in UTF-8, and separated by the semicolon character `;`.

| File | Description |
|---|---|
| `orders.csv` | Order-level information |
| `customer_list.csv` | Customer profile information |
| `order_items.csv` | Product items within each order |
| `order_reviews.csv` | Review scores and feedback |
| `products.csv` | Product metadata |
| `sellers.csv` | Seller metadata |

---

## 5. Tasks and Output Files

| Task | Description | Output file |
|---|---|---|
| 1 | Load datasets and infer schemas | `output/task1_dataset_initializer.txt` |
| 2 | Compute total orders, customers, and sellers | `output/task2_general_statistics.txt` |
| 3 | Count orders by customer country | `output/task3_orders_by_country.txt` |
| 4 | Count orders by year and month | `output/task4_orders_by_year_month.txt` |
| 5 | Compute review score statistics | `output/task5_review_distribution.txt` |
| 7 | Identify top-selling products and average review scores | `output/task7_top_selling_products.txt` |
| 8 | Measure delivery performance | `output/task8_delivery_performance.txt` |
| 9 | Segment customers by order behavior | `output/task9_customer_segmentation.txt` |

---

## 6. Task Descriptions

### Task 1
Validate that the input CSV files can be loaded correctly and that Spark can infer the schema of each dataset.

### Task 2
Compute the core dataset-level statistics, including the total number of orders, customers, and sellers.

### Task 3
Aggregate the number of orders by customer country and sort the results in descending order.

### Task 4
Analyze the temporal distribution of orders by year and month.

### Task 5
Summarize review scores using standard descriptive statistics.

### Task 7
Identify the best-selling products and combine sales with review information to compute the average review score.

### Task 8
Evaluate delivery performance by measuring the delay between the shipping limit date and the carrier delivery date.

### Task 9
Cluster customers into segments according to order frequency, average order value, and purchase regularity.

---

## 7. Build Instructions

From the project root, build the Java module with Maven:

```bash
cd spark/java/lab04
mvn clean package -DskipTests
```

The packaged JAR file will be generated under:

```text
spark/java/lab04/target/
```

---

## 8. Execution Instructions

### Run with `spark-submit`

Example for Task 8:

```bash
spark-submit \
  --class ds200.lab04.task8.Task8_DeliveryPerformance \
  --master local[*] \
  target/lab04-1.0.0.jar \
  data/orders.csv \
  data/order_items.csv \
  output/task8_delivery_performance.txt
```

### Run on Git Bash (Windows)

Example for Task 9:

```bash
spark-submit `
  --class ds200.lab04.task9.Task9_CustomerSegmentation `
  --master local[*] `
  target/lab04-1.0.0.jar `
  data/orders.csv `
  data/customer_list.csv `
  data/order_items.csv `
  output/task9_customer_segmentation.txt
```

---

## 9. Screenshot Helper Script

The file `scripts/screenshot_lab04.sh` prints the environment information and displays the contents of the generated output files in a structured format.  
It is intended for screenshot collection only and does not modify any output data.

---

## 10. Notes

- The output filenames in this README match the screenshot helper script and the generated lab results.
- If a task class name differs from the example above, replace the `--class` value with the actual main class used in the source code.
- The script and README are separated from the Spark jobs themselves; they only help present the already-generated outputs.

---

## 11. Conclusion

This laboratory exercise demonstrates a practical Spark-based workflow for processing structured data at scale.  
The implementation is organized into independent tasks so that each transformation can be verified separately, while the final outputs remain consistent and reproducible.
