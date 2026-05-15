<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 03)</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Java-17%2B-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 17+" />
  <img src="https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark 4.1.1" />
  <img src="https://img.shields.io/badge/RDD-Java-blue?style=for-the-badge" alt="Java RDD" />
  <img src="https://img.shields.io/badge/Platform-Windows%2011-0078D4?style=for-the-badge&logo=windows&logoColor=white" alt="Windows 11" />
</p>

<p align="center">
  <a href="https://github.com/Kun05_AI"><img src="https://img.shields.io/badge/GitHub-Kun05__AI-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/"><img src="https://img.shields.io/badge/LinkedIn-Vu%20Viet%20Cuong-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23520213@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23520213%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab03/`  
**Repository:** `DS200.Q21.1_Lab03`  
**Course:** Phân tích dữ liệu lớn (DS200.Q21)  
**Teaching assistant:** Nguyễn Hiếu Nghĩa  
**Lecturer:** Hà Minh Tân

---

## Student information

| Student ID | Full name | GitHub | Email | LinkedIn |
|:--:|---|---|---|---|
| 23520213 | Vũ Việt Cương | [Kun05_AI](https://github.com/Kun05_AI) | 23520213@gm.uit.edu.vn | [Profile](https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/) |

---

## Objective

This repository contains the solution for **Lab 03** of the Big Data Analysis course. The implementation uses **pure Java Spark RDD** and avoids DataFrame / Dataset APIs entirely. Each task reads plain-text CSV-style files, transforms them with RDD operations, and writes a report to the `output/` directory.

---

## Repository layout

```text
DS200.Q21.1_Lab03/
├── README.md
├── assignments.ipynb
├── data/
│   ├── movies.txt
│   ├── ratings_1.txt
│   ├── ratings_2.txt
│   ├── users.txt
│   └── occupation.txt
├── output/
│   ├── task1_movie_stats.txt
│   ├── task2_genre_stats.txt
│   ├── task3_gender_stats.txt
│   ├── task4_movie_age.txt
│   ├── task5_rating_dist.txt
│   └── task6_year_stats.txt
├── screenshots/
│   └── (task result images)
├── scripts/
│   ├── java.sh
│   └── run_java_rdd_local.sh
└── spark/
    └── java/
        └── lab03-rdd/
            ├── pom.xml
            ├── src/
            │   └── main/
            │       ├── java/
            │       │   └── ds200/lab03/
            │       │       ├── model/
            │       │       ├── util/
            │       │       ├── task1/
            │       │       ├── task2/
            │       │       ├── task3/
            │       │       ├── task4/
            │       │       ├── task5/
            │       │       └── task6/            
            └── target/
```

---

## Dataset

All files are comma-separated.

| File | Schema | Description |
|---|---|---|
| `data/movies.txt` | `MovieID,Title,Genres` | Movie metadata; genres are separated by `|` |
| `data/ratings_1.txt` | `UserID,MovieID,Rating,Timestamp` | First ratings file |
| `data/ratings_2.txt` | `UserID,MovieID,Rating,Timestamp` | Second ratings file; unioned with `ratings_1.txt` |
| `data/users.txt` | `UserID,Gender,Age,Occupation,Zip-code` | User metadata |
| `data/occupation.txt` | `ID,Occupation` | Occupation lookup table |

---

## Environment

| Tool | Version |
|---|---|
| Java | `jdk-17.0.19+10` |
| Apache Spark | `spark-4.1.1-bin-hadoop3` |
| Maven | `apache-maven-3.9.15` |
| Operating system | Windows 11 |

Implementation details:
- Language: **Java**
- Processing model: **Spark RDD**
- Not used: **DataFrame / Dataset / Spark SQL**

---

## Assignments implemented

| Task | Goal | Main output file |
|---|---|---|
| 1 | Average rating and total ratings for each movie; top movie with at least 50 ratings | `output/task1_movie_stats.txt` |
| 2 | Average rating and total ratings by genre | `output/task2_genre_stats.txt` |
| 3 | Average rating and total ratings by movie and gender | `output/task3_gender_stats.txt` |
| 4 | Average rating and total ratings by movie and age group | `output/task4_movie_age.txt` |
| 5 | Rating distribution by movie and rating value | `output/task5_rating_dist.txt` |
| 6 | Average rating and total ratings by year | `output/task6_year_stats.txt` |

---

## Project architecture

### Core layers

| Layer | Responsibility |
|---|---|
| `model/` | Immutable data objects such as `Movie`, `Rating`, `User`, `RatingStats` |
| `util/` | Parsing helpers, Spark context factory, output writer |
| `task1/` to `task6/` | One `main()` per task, each implementing an RDD pipeline |
| `scripts/` | Shell scripts for building and running the whole lab |

### RDD operations used

This project makes use of:
- `map`
- `flatMap`
- `mapToPair`
- `flatMapToPair`
- `reduceByKey`
- `join`
- `union`
- `collectAsMap`
- `broadcast`

The small lookup tables are loaded into driver memory and used as map-style lookups, while the ratings data is processed through RDD aggregations.

---

## End-to-end data flow

```text
input files
   ├── movies.txt
   ├── ratings_1.txt
   ├── ratings_2.txt
   ├── users.txt
   └── occupation.txt
        │
        ├── parse text lines
        ├── build small lookup maps on driver
        ├── union rating files
        ├── map / flatMap / join / reduceByKey
        ├── collect and sort results
        └── write reports to output/*.txt
```

---

## Task details

### Task 1 — Average rating per movie

**Goal:** compute the average rating and total number of ratings for each movie.

**Important rules used in the implementation:**
- `movies.txt` is loaded to map `MovieID -> Title`.
- `ratings_1.txt` and `ratings_2.txt` are unioned.
- For each movie, the code computes:
  - `AverageRating`
  - `TotalRatings`
- Movies with at least **50 ratings** are considered when searching for the top movie.
- In this dataset, no movie reaches 50 ratings, so the top-movie line is reported as `<none>`.

**Output:** `output/task1_movie_stats.txt`

---

### Task 2 — Average rating by genre

**Goal:** compute the average rating and total ratings for each genre.

**Important rules used in the implementation:**
- `movies.txt` is used to map each movie to its genre list.
- Since one movie may contain multiple genres, the rating is expanded to **all genres** of that movie.
- Example: `Crime|Drama` contributes to both `Crime` and `Drama`.

**Output:** `output/task2_genre_stats.txt`

---

### Task 3 — Average rating by gender per movie

**Goal:** compute the average rating and total ratings for each movie separately for male and female users.

**Important rules used in the implementation:**
- `users.txt` is used to map `UserID -> Gender`.
- `movies.txt` is used to map `MovieID -> Title`.
- The final report includes:
  - `MovieID`
  - `Title`
  - `Gender`
  - `AverageRating`
  - `TotalRatings`

**Output:** `output/task3_gender_stats.txt`

---

### Task 4 — Average rating by age group per movie

**Goal:** compute the average rating and total ratings for each movie by age group.

**Age-group rule used in the implementation:**
- `0-17`
- `18-24`
- `25-34`
- `35-44`
- `45-49`
- `50-55`
- `56+`

**Important rules used in the implementation:**
- `users.txt` is used to map `UserID -> AgeGroup`.
- `movies.txt` is used to map `MovieID -> Title`.
- The final report includes:
  - `MovieID`
  - `Title`
  - `AgeGroup`
  - `AverageRating`
  - `TotalRatings`

**Output:** `output/task4_movie_age.txt`

---

### Task 5 — Rating distribution by movie

**Goal:** count how many times each rating value appears for each movie.

**Important rules used in the implementation:**
- Ratings are grouped by `(MovieID, RatingValue)`.
- `movies.txt` is used to map `MovieID -> Title`.
- The rating value is formatted consistently to four decimal places.
- The report is sorted by `MovieID` and then by rating value.

**Output:** `output/task5_rating_dist.txt`

---

### Task 6 — Average rating by year

**Goal:** compute the average rating and total ratings for each year.

**Important rules used in the implementation:**
- Both ratings files are unioned.
- The Unix timestamp is converted to year in UTC.
- The final report includes:
  - `Year`
  - `AverageRating`
  - `TotalRatings`

**Output:** `output/task6_year_stats.txt`

---

## Prerequisites

Before running the project, make sure the following tools are available:

- Java 17
- Maven 3.9.15
- Apache Spark 4.1.1

Check commands:

```bash
java -version
mvn -version
spark-submit --version
```

---

## Build and run

### Build

From the project root:

```bash
cd spark/java/lab03-rdd
mvn clean package -DskipTests
```

### Run all tasks

If you want to run everything through the helper script:

```bash
bash scripts/run_java_rdd_local.sh
```

### Run individually on Windows PowerShell

Example command style:

```powershell
$env:SPARK_HOME = "E:\D\UIT\Java\spark-4.1.1-bin-hadoop3"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --master local[*] --class ds200.lab03.task1.Task1App `
  "spark/java/lab03-rdd/target/spark-rdd-lab03-1.0-SNAPSHOT.jar" `
  "data/movies.txt" `
  "data/ratings_1.txt" `
  "data/ratings_2.txt" `
  "output/task1_movie_stats.txt" 50
```

The same pattern is used for the other tasks by changing the `--class` and output file.

---

## Output files

After running the project, the following output files are produced:

- `output/task1_movie_stats.txt`
- `output/task2_genre_stats.txt`
- `output/task3_gender_stats.txt`
- `output/task4_movie_age.txt`
- `output/task5_rating_dist.txt`
- `output/task6_year_stats.txt`

---

## Screenshots

Screenshots of the output results are stored in the `screenshots/` folder.

Suggested naming convention:
- `screenshots/task1_movie_stats.png`
- `screenshots/task2_genre_stats.png`
- `screenshots/task3_gender_stats.png`
- `screenshots/task4_movie_age.png`
- `screenshots/task5_rating_dist.png`
- `screenshots/task6_year_stats.png`

---

## Implementation notes

- The project is implemented using **RDD only**.
- Small lookup files are loaded into the driver as maps because they are tiny compared with the ratings data.
- The code uses `reduceByKey` for aggregation.
- `join` is used where user or movie metadata must be attached to ratings.
- `flatMapToPair` is used in places where one record contributes to multiple keys, such as genres.
- Outputs are formatted as plain text for easy submission and review.

---

## Checklist

- [x] Java Spark RDD implementation completed
- [x] All six tasks implemented
- [x] Output files generated in `output/`
- [x] Screenshots stored in `screenshots/`
- [x] README written for the current repository
- [x] Project structure organized for submission

---

## Notes

This repository is a course lab submission for **Phân tích dữ liệu lớn (DS200.Q21)** at the University of Information Technology.

The project demonstrates:
- RDD-based data processing
- group-by style aggregation
- file-based reporting
- Spark job execution with Maven and `spark-submit`

