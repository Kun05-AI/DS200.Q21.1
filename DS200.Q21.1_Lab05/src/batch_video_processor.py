import os
import sys
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    print("[Lỗi] Thư viện OpenCV bắt buộc phải có để chạy tập lệnh này.")
    sys.exit(1)

try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("[Lỗi] Thư viện Ultralytics bắt buộc phải có để chạy tập lệnh này.")
    sys.exit(1)

try:
    from sahi import AutoDetectionModel
    from sahi.predict import get_sliced_prediction
    SAHI_AVAILABLE = True
except ImportError:
    SAHI_AVAILABLE = False
    print("[Thông tin] Không tìm thấy SAHI. Chạy chế độ suy luận mặc định.")

try:
    from pyspark import SparkContext, SparkConf
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("[Thông tin] Không tìm thấy Spark. Chuyển đổi sang vòng lặp xử lý tuần tự nội bộ.")

from settings import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("BatchVideoProcessor")


def process_single_video_pipeline(video_path: str, model_path: str, out_dir: str, enable_sahi: bool, conf_thresh: float) -> Dict[str, Any]:
    """Xử lý một video và ghi ra summary/frame_detections/report."""
    v_name = Path(video_path).stem
    logger.info(f"Bắt đầu xử lý video: {v_name}")

    capture = cv2.VideoCapture(video_path)
    if not capture.isOpened():
        logger.error(f"Lỗi mở video: {video_path}")
        return {"video_name": v_name, "error": "Cannot open video file"}

    tot_frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
    video_fps = capture.get(cv2.CAP_PROP_FPS)
    w = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    duration = tot_frames / video_fps if video_fps > 0 else 0

    net_model = YOLO(model_path)
    sahi_model_node = None
    if enable_sahi and SAHI_AVAILABLE:
        sahi_model_node = AutoDetectionModel.from_pretrained(
            model_type="ultralytics",
            model_path=model_path,
            confidence_threshold=conf_thresh,
            device="cpu",
        )

    frame_idx = 0
    accumulated_detections = 0
    frames_containing_people = 0
    max_count_in_frame = 0
    records_list = []

    t_start = datetime.now()

    while True:
        status, img_frame = capture.read()
        if not status:
            break

        frame_idx += 1
        current_count = 0
        boxes_in_frame = []

        if enable_sahi and sahi_model_node:
            sahi_res = get_sliced_prediction(
                img_frame, sahi_model_node, slice_height=256, slice_width=256, verbose=0
            )
            for pred in sahi_res.object_prediction_list:
                if pred.category.id == 0 or pred.category.name == "person":
                    current_count += 1
                    b = pred.bbox
                    boxes_in_frame.append({
                        "x": int(b.xmin),
                        "y": int(b.ymin),
                        "width": int(b.maxx - b.xmin),
                        "height": int(b.maxy - b.ymin),
                        "confidence": float(pred.score.value),
                    })
        else:
            yolo_res = net_model(img_frame, verbose=False)[0]
            for b_info in yolo_res.boxes:
                if int(b_info.cls[0]) == 0 and float(b_info.conf[0]) >= conf_thresh:
                    current_count += 1
                    c = b_info.xyxy[0].tolist()
                    boxes_in_frame.append({
                        "x": int(c[0]),
                        "y": int(c[1]),
                        "width": int(c[2] - c[0]),
                        "height": int(c[3] - c[1]),
                        "confidence": round(float(b_info.conf[0]), 3),
                    })

        accumulated_detections += current_count
        if current_count > 0:
            frames_containing_people += 1
        if current_count > max_count_in_frame:
            max_count_in_frame = current_count

        records_list.append({
            "frame_number": frame_idx,
            "timestamp": round(frame_idx / video_fps, 4) if video_fps > 0 else 0,
            "person_count": current_count,
            "bounding_boxes": boxes_in_frame,
            "detection_method": "SAHI + YOLO" if enable_sahi else "YOLO",
        })

    capture.release()
    total_processing_seconds = (datetime.now() - t_start).total_seconds()
    processing_fps = frame_idx / total_processing_seconds if total_processing_seconds > 0 else 0

    output_summary = {
        "video_name": v_name,
        "video_path": video_path,
        "video_info": {
            "total_frames": tot_frames,
            "fps": round(video_fps, 4),
            "resolution": f"{w}x{h}",
            "duration_seconds": round(duration, 2),
        },
        "detection_summary": {
            "total_detections": accumulated_detections,
            "frames_processed": frame_idx,
            "avg_persons_per_frame": round(accumulated_detections / frame_idx, 2) if frame_idx > 0 else 0,
            "max_persons_in_frame": max_count_in_frame,
            "frames_with_persons": frames_containing_people,
            "detection_method": "SAHI + YOLO" if enable_sahi else "YOLO",
        },
        "processing": {
            "processing_time_seconds": round(total_processing_seconds, 2),
            "fps_processing": round(processing_fps, 2),
            "timestamp": datetime.now().isoformat(),
        },
    }

    video_out_dir = os.path.join(out_dir, v_name)
    os.makedirs(video_out_dir, exist_ok=True)

    with open(os.path.join(video_out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(output_summary, f, indent=2, ensure_ascii=False)

    with open(os.path.join(video_out_dir, "frame_detections.json"), "w", encoding="utf-8") as f:
        json.dump(records_list, f, indent=2, ensure_ascii=False)

    with open(os.path.join(video_out_dir, "report.txt"), "w", encoding="utf-8") as f:
        f.write(f"Video Analysis Report: {v_name}\n" + "=" * 50 + "\n")
        f.write(f"Video Info:\n  - Resolution: {w}x{h}\n  - FPS: {video_fps}\n")
        f.write(f"  - Total Frames: {tot_frames}\n  - Duration: {duration:.2f} seconds\n\n")
        f.write(f"Detection Results:\n  - Detection Method: {'SAHI + YOLO' if enable_sahi else 'YOLO'}\n")
        f.write(f"  - Total Persons Detected: {accumulated_detections}\n")
        f.write(f"  - Average Persons/Frame: {accumulated_detections / frame_idx if frame_idx > 0 else 0:.1f}\n")
        f.write(f"  - Max Persons in Single Frame: {max_count_in_frame}\n")
        f.write(f"  - Frames with Persons: {frames_containing_people}/{frame_idx}\n\n")
        f.write(f"Processing:\n  - Processing Time: {total_processing_seconds:.2f} seconds\n")
        f.write(f"  - Processing FPS: {processing_fps:.2f}\n")

    return output_summary


def execute_batch_processing(video_list: List[str], model: str, output: str, use_sahi: bool, confidence: float) -> List[Dict[str, Any]]:
    """Chạy batch bằng Spark nếu có, không thì fallback tuần tự."""
    if SPARK_AVAILABLE:
        try:
            logger.info("Khởi chạy Spark batch processing...")
            conf = SparkConf().setAppName(Config.SPARK_APP_NAME).setMaster(Config.SPARK_MASTER)
            sc = SparkContext.getOrCreate(conf=conf)

            video_rdd = sc.parallelize(video_list, len(video_list))
            pipeline_results = video_rdd.map(
                lambda filepath: process_single_video_pipeline(filepath, model, output, use_sahi, confidence)
            ).collect()
            return pipeline_results
        except Exception as e:
            logger.warning(f"Spark không chạy được, fallback sang sequential: {e}")

    logger.info("Chạy tuần tự.")
    seq_results = []
    for path in video_list:
        seq_results.append(process_single_video_pipeline(path, model, output, use_sahi, confidence))
    return seq_results


def main():
    parser = argparse.ArgumentParser(description="Batch Processing video với YOLO/SAHI.")
    parser.add_argument("--videos-dir", default="")
    parser.add_argument("--output-dir", default=Config.RESULTS_DIR)
    parser.add_argument("--model", default=Config.YOLO_MODEL_PATH)
    parser.add_argument("--confidence", type=float, default=Config.CONFIDENCE_THRESHOLD)
    parser.add_argument("--no-sahi", action="store_true")

    args = parser.parse_args()

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    v_dir = args.videos_dir if args.videos_dir else os.path.join(project_root, "data", "video")
    os.makedirs(args.output_dir, exist_ok=True)

    valid_ext = (".mp4", ".avi", ".mov", ".mkv")
    found_videos = []

    if os.path.exists(v_dir):
        for f in os.listdir(v_dir):
            if f.lower().endswith(valid_ext):
                found_videos.append(os.path.join(v_dir, f))

    if not found_videos:
        logger.warning(f"Không tìm thấy video hợp lệ tại: {v_dir}")
        return

    found_videos.sort()
    logger.info(f"Tìm thấy {len(found_videos)} video cần xử lý.")

    run_sahi = not args.no_sahi
    start_time = datetime.now()

    final_output_list = execute_batch_processing(found_videos, args.model, args.output_dir, run_sahi, args.confidence)
    duration_total = (datetime.now() - start_time).total_seconds()

    global_summary = {
        "processing_info": {
            "total_videos": len(found_videos),
            "total_processing_time": round(duration_total, 2),
            "detection_method": "SAHI + YOLO" if run_sahi else "YOLO",
            "spark_enabled": SPARK_AVAILABLE,
            "timestamp": datetime.now().isoformat(),
        },
        "videos": final_output_list,
    }

    with open(os.path.join(args.output_dir, "overall_summary.json"), "w", encoding="utf-8") as writer:
        json.dump(global_summary, writer, indent=2, ensure_ascii=False)

    print("\n" + "=" * 60 + "\n  HOÀN THÀNH BATCH PROCESSING\n" + "=" * 60)
    print(f"Tổng số video: {len(found_videos)} | Tổng thời gian: {duration_total:.2f} giây")


if __name__ == "__main__":
    main()