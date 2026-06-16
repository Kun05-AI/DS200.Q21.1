<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 05)</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Java-17%2B-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 17+" />
  <img src="https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark 4.1.1" />
  <img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.x" />
  <img src="https://img.shields.io/badge/YOLO-Ultralytics-00FFFF?style=for-the-badge&logo=yolo&logoColor=black" alt="YOLO" />
  <img src="https://img.shields.io/badge/Platform-Windows%2011-0078D4?style=for-the-badge&logo=windows&logoColor=white" alt="Windows 11" />
</p>

<p align="center">
  <a href="https://github.com/Kun05-AI"><img src="https://img.shields.io/badge/GitHub-Kun05__AI-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/"><img src="https://img.shields.io/badge/LinkedIn-Vu%20Viet%20Cuong-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23520213@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23520213%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab05/`  
**Repository:** `DS200.Q21.1_Lab05`  
**Course:** Big Data Analysis (DS200.Q21)  
**Teaching assistant:** Nguyễn Hiếu Nghĩa  
**Lecturer:** Hà Minh Tân

---

## Student information

| Student ID | Full name | GitHub | Email | LinkedIn |
|:--:|---|---|---|---|
| 23520213 | Vũ Việt Cương | [Kun05_AI](https://github.com/Kun05-AI) | 23520213@gm.uit.edu.vn | [Profile](https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/) |

---

<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 05)</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Java-17%2B-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 17+" />
  <img src="https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark 4.1.1" />
  <img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.x" />
  <img src="https://img.shields.io/badge/YOLO-Ultralytics-00FFFF?style=for-the-badge&logo=yolo&logoColor=black" alt="YOLO" />
  <img src="https://img.shields.io/badge/Platform-Windows%2011-0078D4?style=for-the-badge&logo=windows&logoColor=white" alt="Windows 11" />
</p>

<p align="center">
  <a href="https://github.com/Kun05-AI"><img src="https://img.shields.io/badge/GitHub-Kun05__AI-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/"><img src="https://img.shields.io/badge/LinkedIn-Vu%20Viet%20Cuong-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23520213@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23520213%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab05/`
**Repository:** `DS200.Q21.1_Lab05`
**Course:** Big Data Analysis (DS200.Q21)
**Teaching assistant:** Nguyễn Hiếu Nghĩa
**Lecturer:** Hà Minh Tân

---

## Student information

| Student ID | Full name     | GitHub                                  | Email                                                   | LinkedIn                                                                  |
| :--------: | ------------- | --------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------- |
|  23520213  | Vũ Việt Cương | [Kun05_AI](https://github.com/Kun05-AI) | [23520213@gm.uit.edu.vn](mailto:23520213@gm.uit.edu.vn) | [Profile](https://www.linkedin.com/in/c%C6%B0%C6%A1ng-v%C5%A9-670a76365/) |

---

## Objective

This laboratory assignment implements a distributed person-counting system capable of receiving image frames from a video source, performing real-time object detection, and storing structured detection results for subsequent analysis.

The system is designed as a multi-service architecture consisting of independent components communicating through TCP sockets. Each component is responsible for a specific stage of the processing pipeline, including frame acquisition, frame forwarding, object detection, and result storage.

To satisfy the Big Data Analysis context of the course, the project adopts a scalable processing design where data generation, processing, and storage are separated into dedicated services. In addition, batch processing functionality is provided through Apache Spark to support large-scale video analysis workloads.

The primary objectives of this lab are:

* Build a distributed pipeline for real-time frame transmission.
* Detect and count people appearing in video streams.
* Extract and store bounding-box information for detected objects.
* Persist processing results in structured formats for further analytics.
* Demonstrate scalable processing principles in a Big Data environment.

---

## Lab Requirements

According to the assignment specification, the system must provide the following functionalities:

1. A server responsible for receiving image frames from a camera or video source and forwarding them to the processing server.
2. A processing server responsible for object detection and generation of bounding boxes representing detected objects.
3. A storage server responsible for receiving and persisting detection results.
4. Source code and generated outputs committed to GitHub for evaluation.
5. The overall solution implemented within a Big Data processing context.

The implemented solution satisfies these requirements through a modular service-oriented architecture and a scalable processing workflow.

---

## Dataset

The experimental dataset consists of **2 MP4 videos** containing pedestrians moving back and forth in the monitored scene.

| Video        | Description                                   |
| ------------ | --------------------------------------------- |
| `Nguoi1.mp4` | Video of pedestrians moving through the scene |
| `Nguoi2.mp4` | Video of pedestrians moving through the scene |

Dataset location:

```text
DS200.Q21.1_Lab05/
└── data/
    └── video/
        ├── Nguoi1.mp4
        └── Nguoi2.mp4
```

These videos are used as input streams for evaluating the person-counting pipeline and generating detection statistics.

---

## System Architecture

The system is organized into four major components.

```text
+----------------+
| Frame Sender   |
+----------------+
        |
        | TCP / JSON
        v
+----------------+
| Frame Receiver |
+----------------+
        |
        | TCP / JSON
        v
+----------------+
| Detector Server|
| YOLO + SAHI    |
+----------------+
        |
        | TCP / JSON
        v
+----------------+
| Storage Server |
+----------------+
        |
        v
 Detection Results
 Reports
 Screenshots
 JSON Summaries
```

### Frame Sender

The sender component is responsible for:

* Reading frames from a video source.
* Converting image frames into JPEG format.
* Encoding image data as Base64.
* Packaging frame information into JSON messages.
* Transmitting frames to the receiver server.

### Frame Receiver

The receiver server acts as an intermediate communication layer.

Responsibilities include:

* Accepting incoming frame streams.
* Parsing JSON messages.
* Monitoring frame traffic.
* Forwarding frames to the detector server.

### Detector Server

The detector server performs the core computer vision processing tasks.

Responsibilities include:

* Decoding image frames.
* Running person detection using YOLO.
* Supporting optional SAHI sliced inference.
* Applying Non-Maximum Suppression (NMS).
* Counting detected persons.
* Extracting bounding boxes.
* Sending structured detection results to the storage server.

### Storage Server

The storage server manages persistence and reporting.

Responsibilities include:

* Receiving detection outputs.
* Storing raw detection records.
* Generating per-video summaries.
* Producing overall processing statistics.
* Creating human-readable reports.

---

## Repository Structure

```text
DS200.Q21.1_Lab05/
├── README.md
├── requirements.txt
├── data/
│   └── video/
│       ├── Nguoi1.mp4
│       └── Nguoi2.mp4
├── models/
│   └── yolo/
│       └── yolo12n.pt
├── output/
│   ├── results/
│   │   ├── detections_raw.jsonl
│   │   ├── overall_summary.json
│   │   ├── Nguoi1/
│   │   │   ├── frame_detections.json
│   │   │   ├── report.txt
│   │   │   └── summary.json
│   │   └── Nguoi2/
│   │       ├── frame_detections.json
│   │       ├── report.txt
│   │       └── summary.json
│   └── screenshots/
│       ├── no-sahi/
│       │   ├── Nguoi1/
│       │   └── Nguoi2/
│       └── with-sahi/
│           ├── Nguoi1/
│           └── Nguoi2/
├── scripts/
│   ├── start_bg_remove.sh
│   ├── start_demo_pipeline.sh
│   ├── start_detector.sh
│   ├── start_mediapipe_test.sh
│   ├── start_pipeline.sh
│   ├── start_receiver.sh
│   ├── start_screenshot_compare.sh
│   ├── start_sender.sh
│   └── start_storage.sh
└── src/
    ├── __init__.py
    ├── background_remove_tool.py
    ├── batch_video_processor.py
    ├── detector_server.py
    ├── end_to_end_demo.py
    ├── frame_receiver.py
    ├── frame_sender.py
    ├── mediapipe_test_lab.py
    ├── result_storage_server.py
    ├── screenshot_comparator.py
    └── settings.py
```

> Note: the committed repository focuses on executable code and result artifacts. Input videos and images are expected to be placed in the runtime input folders used by the scripts, typically under `data/video/` and `data/images/` when running locally.

---

## Technologies Used

| Technology   | Purpose                                     |
| ------------ | ------------------------------------------- |
| Python       | Main implementation language                |
| OpenCV       | Video processing and image manipulation     |
| YOLO         | Person detection                            |
| SAHI         | Sliced inference for small-object detection |
| Apache Spark | Batch processing and scalable execution     |
| TCP Socket   | Inter-service communication                 |
| JSON         | Message serialization                       |
| NumPy        | Numerical processing                        |
| MediaPipe    | Experimental computer vision utilities      |

---

## Processing Workflow

The complete processing workflow is summarized below.

```text
+-------------------+           TCP Network           +------------------------+
|  tcp_example.py   |  ---------------------------->  |    spark_stream.py     |
| (Stream Server)   |         Port: 9999              | (Spark Streaming Consumer)
+-------------------+                                 +------------------------+
         |                                                         |
         v                                                         v
   [Video Source]                                         [Micro-batch RDDs]
                                                                   |
                                                                   v
                                                      +------------------------+
                                                      |   yolo_detector.py     |
                                                      |  (Inference Engine)    |
                                                      +------------------------+
                                                                   |
                                                                   v
                                                      +------------------------+
                                                      |    output/ Thư mục     |
                                                      | (Rendered Video/Image) |
                                                      +------------------------+
```

---

## Message Format

The system uses newline-delimited JSON (`NDJSON`) messages for transport.

### Sender to Receiver

```json
{
  "type": "FRAME",
  "payload": {
    "frame_id": 1,
    "timestamp": 1710000000.0,
    "image_data": "<base64-jpeg>",
    "video_name": "Nguoi1",
    "video_meta": {
      "source_type": "video",
      "source_path": "...",
      "fps": 25,
      "width": 3840,
      "height": 2160,
      "total_frames": 466
    }
  }
}
```

### Receiver to Detector

The receiver forwards the normalized frame payload to the detector server.

### Detector to Storage

```json
{
  "video_name": "Nguoi1",
  "video_meta": {},
  "frame_number": 1,
  "timestamp": 1710000000.0,
  "person_count": 8,
  "bounding_boxes": [
    {
      "x": 120,
      "y": 80,
      "width": 210,
      "height": 420,
      "confidence": 0.91,
      "label": "person"
    }
  ],
  "processing_time_ms": 54.21,
  "detection_method": "YOLO"
}
```

---

## Output Artifacts

After running the pipeline, the storage layer generates the following artifacts:

### Global outputs

* `output/results/detections_raw.jsonl`
* `output/results/overall_summary.json`

### Per-video outputs

* `output/results/<video_name>/frame_detections.json`
* `output/results/<video_name>/summary.json`
* `output/results/<video_name>/report.txt`

### Visual evidence

* `output/screenshots/no-sahi/<video_name>/frame_XXXX.jpg`
* `output/screenshots/with-sahi/<video_name>/frame_XXXX.jpg`

These screenshots provide a direct visual comparison between plain YOLO inference and SAHI-based inference.

---

## Sample Committed Results

The repository includes committed outputs for two videos:

* `Nguoi1.mp4`
* `Nguoi2.mp4`

From the generated summaries, the sample run contains:

* **2 videos**
* **750 total frames processed**
* **6435 total person detections**
* **8.58 persons/frame overall**

Per-video summary:

* `Nguoi1.mp4`: 292 frames, 2263 detections, 7.75 persons/frame
* `Nguoi2.mp4`: 458 frames, 4172 detections, 9.11 persons/frame

These files are already committed under `output/results/` and can be used as evidence for lab submission.

---

## Dependencies

Install dependencies with:

```bash
pip install -r requirements.txt
```

Main dependencies include:

* `pyspark`
* `opencv-python`
* `numpy`
* `ultralytics`
* `sahi`
* `mediapipe`
* `pillow`

---

## Installation

### Using a virtual environment

```bash
python -m venv .venv
```

### Activate the environment

On Linux, macOS, or Git Bash:

```bash
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### Install packages

```bash
pip install -r requirements.txt
```

---

## Configuration

All core parameters are centralized in `src/settings.py`.

### Default ports

* Receiver: `6100`
* Detector: `6200`
* Storage: `6300`

### Other defaults

* buffer size: `64 KB`
* YOLO confidence threshold: `0.3`
* snapshot interval: `50` frames
* Spark master: `local[*]`
* Spark application name: `PersonCountingStream`

These defaults can be changed directly in `settings.py` or overridden via command-line arguments supported by each script.

---

## How to Run

### 1. Run the full pipeline

The pipeline order is:

1. storage server
2. detector server
3. receiver server
4. sender

Use the helper script:

```bash
bash scripts/start_pipeline.sh
```

### 2. Run each component separately

Start the storage server:

```bash
bash scripts/start_storage.sh
```

Start the detector server:

```bash
bash scripts/start_detector.sh
```

Start the receiver:

```bash
bash scripts/start_receiver.sh
```

Start the sender:

```bash
bash scripts/start_sender.sh --video data/video/Nguoi1.mp4 --frames 200 --fps 5
```

### 3. Run the end-to-end demo

```bash
bash scripts/start_demo_pipeline.sh --video data/video/Nguoi1.mp4 --frames 10
```

### 4. Batch process a folder of videos

```bash
python src/batch_video_processor.py --videos-dir data/video --output-dir output/results
```

### 5. Generate YOLO vs SAHI comparison screenshots

```bash
bash scripts/start_screenshot_compare.sh
```

### 6. Run background removal utility

```bash
bash scripts/start_bg_remove.sh --input data/images/sample.jpg --output output/bg_removed.jpg
```

### 7. Run MediaPipe utility

```bash
bash scripts/start_mediapipe_test.sh --input data/images/sample.jpg --mode all
```

> The provided shell scripts are designed for Bash-compatible environments such as Linux, macOS, Git Bash, or WSL.

---

## Implementation Notes

### Transport design

The system uses TCP sockets and newline-delimited JSON to ensure simple interoperability between services.

### Inference strategy

* **YOLO** is the default person detector.
* **SAHI** is optionally enabled to improve detection in high-resolution scenes or when small persons appear in the frame.
* **NMS** is applied after detection to reduce duplicated bounding boxes.

### Storage design

The storage layer uses a `Queue` and a dedicated writer thread so that network reception is not blocked by file I/O.

### Big data context

Although the current execution defaults to a local environment, the system is designed with a big-data mindset:

* the pipeline is decomposed into independent services,
* results are persisted in structured machine-readable formats,
* batch analysis can exploit Spark RDD parallelism,
* processing can be scaled by distributing services across machines if needed.

---

## Reproducibility Checklist

* [x] Source code committed to GitHub
* [x] Result files committed to GitHub
* [x] Real-time pipeline implemented
* [x] YOLO-based person detection implemented
* [x] Optional SAHI inference implemented
* [x] Storage server produces structured outputs
* [x] Screenshots of results are included
* [x] Batch processing support is available

---

## Conclusion

This lab demonstrates an end-to-end person-counting pipeline for video streams in a Big Data context. The solution combines real-time TCP communication, deep-learning inference, structured persistence, and optional Spark-based batch processing to satisfy the lab requirement while keeping the architecture modular, extensible, and suitable for future scaling.


