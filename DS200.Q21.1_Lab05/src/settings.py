import os

# Đường dẫn gốc của toàn bộ dự án
ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class MessageType:
    FRAME = "FRAME"
    DETECTION_RESULT = "DETECTION_RESULT"
    HEARTBEAT = "HEARTBEAT"


class Config:
    """Cấu hình trung tâm cho toàn bộ hệ thống."""

    HOST = "localhost"

    # Các cổng TCP
    RECEIVER_PORT = 6100
    PROCESSING_PORT = 6200
    STORAGE_PORT = 6300

    # Kích thước bộ đệm TCP
    BUFFER_SIZE = 64 * 1024

    # Spark
    SPARK_APP_NAME = "PersonCountingStream"
    SPARK_MASTER = "local[*]"
    SPARK_BATCH_INTERVAL = 1

    # Nhận diện đối tượng
    CONFIDENCE_THRESHOLD = 0.3
    YOLO_MODEL_PATH = os.path.join(ROOT_DIRECTORY, "models", "yolo", "yolo12n.pt")

    # Thư mục output chuẩn mới
    OUTPUT_DIR = os.path.join(ROOT_DIRECTORY, "output")
    RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")
    SCREENSHOTS_DIR = os.path.join(OUTPUT_DIR, "screenshots")

    # File raw lưu tạm ở storage server
    STORAGE_FILE = os.path.join(RESULTS_DIR, "detections_raw.jsonl")

    # Dữ liệu vào
    DATA_DIR = os.path.join(ROOT_DIRECTORY, "data")
    VIDEO_DIR = os.path.join(DATA_DIR, "video")
    IMAGE_DIR = os.path.join(DATA_DIR, "images")

    # Logging
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s -> %(message)s"