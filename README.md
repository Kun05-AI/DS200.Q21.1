<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Java-17%2B-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 17+" />
  <img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.x" />
  <img src="https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark 4.1.1" />
  <img src="https://img.shields.io/badge/Platform-Windows%2011-0078D4?style=for-the-badge&logo=windows&logoColor=white" alt="Windows 11" />
</p>

<p align="center">
  <a href="https://github.com/Kun05-AI"><img src="https://img.shields.io/badge/GitHub-Kun05__AI-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/"><img src="https://img.shields.io/badge/LinkedIn-Vu%20Viet%20Cuong-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23520213@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23520213%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1/`  
**Repository:** `DS200.Q21.1`  
**Course:** Big Data Analysis (DS200.Q21)  
**Teaching assistant:** Nguyễn Hiếu Nghĩa  
**Lecturer:** Hà Minh Tân

---

## Student information

| Student ID | Full name | GitHub | Email | LinkedIn |
|:--:|---|---|---|---|
| 23520213 | Vũ Việt Cương | [Kun05_AI](https://github.com/Kun05-AI) | 23520213@gm.uit.edu.vn | [Profile](https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/) |

---

## Overview

This repository contains the solutions for three laboratory assignments of the **Big Data Analysis** course.

The labs focus on different but related topics in big data processing and system design:

- **Lab 03:** distributed data analysis using **Apache Spark RDD**
- **Lab 04:** structured data processing using **Apache Spark DataFrame/Dataset**
- **Lab 05:** distributed person counting and object detection using a multi-service pipeline

Together, these labs demonstrate practical skills in batch processing, structured querying, distributed communication, and scalable computer vision workflows.

---

## Lab 03 — Spark RDD Movie Rating Analysis

Lab 03 implements a Spark RDD-based solution for analyzing movie rating data. The workflow reads plain-text input files, performs transformations and aggregations with RDD operations, and writes results to the `output/` directory. The implementation avoids DataFrame and Dataset APIs in order to practice low-level distributed data processing.

### Main Characteristics

- RDD-only implementation
- CSV-style text input
- Aggregation by movie, genre, gender, age group, rating value, and year
- Result files written as text reports
- Screenshots included for output verification

### Main Data Files

- `movies.txt`
- `ratings_1.txt`
- `ratings_2.txt`
- `users.txt`
- `occupation.txt`

### Main Tasks

- Average rating and rating count by movie
- Average rating and rating count by genre
- Average rating and rating count by gender
- Average rating and rating count by age group
- Rating distribution by movie
- Average rating and rating count by year

---

## Lab 04 — Spark DataFrame/Dataset Analysis

Lab 04 focuses on structured data processing using Apache Spark through the DataFrame/Dataset API. Compared with Lab 03, this lab emphasizes schema-aware transformations, declarative querying, and higher-level abstractions for large-scale data analytics.

### Main Characteristics

- Spark DataFrame/Dataset implementation
- Structured schema-based processing
- SQL-like transformations and aggregations
- Result reporting and screenshots for validation

### Main Focus

- Data ingestion and schema definition
- Transformation using Spark SQL-style operations
- Aggregation and grouping on structured datasets
- Generation of output files for submission

---

## Lab 05 — Distributed Person Counting System

Lab 05 implements a distributed video-processing system for counting people appearing in camera frames or video streams. The system is organized into independent services that communicate through TCP sockets.

### Main Components

- **Frame Sender:** Reads frames from video input and sends them to the receiver.
- **Frame Receiver:** Forwards incoming frame messages to the detector.
- **Detector Server:** Performs person detection using YOLO, with optional SAHI-based sliced inference.
- **Storage Server:** Stores detection results and generates summaries.

### Main Characteristics

- Multi-service distributed architecture
- TCP/JSON message passing
- YOLO-based person detection
- Optional SAHI for improved small-object detection
- Structured persistence of detection results
- Batch processing support with Apache Spark in a Big Data context

### Dataset

The lab uses two MP4 videos:

- `Nguoi1.mp4`
- `Nguoi2.mp4`

These videos contain pedestrian movement scenes and are used to evaluate the person-counting pipeline.

---

## Technologies Used

| Technology | Purpose |
|------------|----------|
| Java | Core implementation for Spark-based labs |
| Python | Main implementation language for Lab 05 |
| Apache Spark | Distributed batch processing |
| RDD API | Low-level distributed data processing |
| DataFrame/Dataset API | Structured processing in Lab 04 |
| OpenCV | Video and frame handling |
| YOLO | Person detection |
| SAHI | Sliced inference for object detection |
| TCP / JSON | Service communication |
| Maven | Build management for Java projects |

---

## Common Project Goals

Across all three labs, the repository demonstrates the following skills:

- Distributed data processing with Spark
- Structured and unstructured data handling
- Aggregation and reporting
- System modularity and pipeline design
- Reproducible result generation
- Academic documentation and GitHub submission workflow

---

## Environment

| Tool | Version / Note |
|------|----------------|
| Java | 17+ |
| Python | 3.x |
| Apache Spark | 4.1.1 |
| Operating System | Windows 11 |
| Main Languages | Java, Python |

---

## Notes

Each lab folder contains its own source code, scripts, outputs, screenshots, and documentation.

The repository is intended for course submission and laboratory reporting in the Big Data Analysis course at the University of Information Technology (UIT), Vietnam National University Ho Chi Minh City.

---

## Conclusion

This repository collects three laboratory assignments that progressively cover Spark RDD processing, Spark DataFrame/Dataset processing, and a distributed computer vision system for person counting. Together, these projects provide practical experience in Big Data technologies, distributed computing, structured analytics, and scalable system design.

---

## Repository structure

```text
DS200.Q21.1/
├── DS200.Q21.1_Lab03/
├── DS200.Q21.1_Lab04/
└── DS200.Q21.1_Lab05/