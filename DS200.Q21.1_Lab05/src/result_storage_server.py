import socket
import json
import logging
import threading
import os
import queue
from collections import defaultdict
from datetime import datetime

from settings import Config, MessageType

logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("DataStorageServer")


class StorageServer:
    """Nhận kết quả detection và ghi ra output/results/<video>/..."""

    def __init__(self, host=Config.HOST, port=Config.STORAGE_PORT, flush_batch_size: int = 10):
        self.host_address = host
        self.listen_port = port
        self.listener_socket = None
        self.is_running = False

        self.flush_batch_size = max(1, flush_batch_size)
        
        # Hàng đợi (Queue) giúp tách biệt việc nhận mạng và ghi file
        self.data_queue = queue.Queue()
        self.writer_thread = None

        self.records_by_video = defaultdict(list)
        self.video_meta_by_name = {}
        self._ensure_storage_space()

    def _ensure_storage_space(self):
        os.makedirs(Config.RESULTS_DIR, exist_ok=True)
        os.makedirs(Config.SCREENSHOTS_DIR, exist_ok=True)

        folder_path = os.path.dirname(Config.STORAGE_FILE)
        if folder_path:
            os.makedirs(folder_path, exist_ok=True)

    def start(self):
        """Khởi động storage server."""
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind((self.host_address, self.listen_port))
        self.listener_socket.listen(5)
        self.is_running = True

        logger.info("=" * 60)
        logger.info(f"STORAGE SERVER đang chạy tại {self.host_address}:{self.listen_port}")
        logger.info("=" * 60)

        # Khởi chạy luồng ngầm chuyên biệt cho việc lưu file
        self.writer_thread = threading.Thread(target=self._disk_writer_worker, daemon=True)
        self.writer_thread.start()

        while self.is_running:
            try:
                client_conn, client_addr = self.listener_socket.accept()
                logger.info(f"Storage nhận kết nối từ: {client_addr}")
                t = threading.Thread(target=self._process_data_session, args=(client_conn,), daemon=True)
                t.start()
            except Exception as e:
                if self.is_running:
                    logger.error(f"Lỗi accept connection: {e}")

    def _process_data_session(self, conn):
        """Nhận data line-based JSON và đẩy vào hàng đợi (Queue)."""
        session_buffer = ""
        try:
            while self.is_running:
                bytes_received = conn.recv(Config.BUFFER_SIZE)
                if not bytes_received:
                    break

                session_buffer += bytes_received.decode("utf-8", errors="ignore")

                while "\n" in session_buffer:
                    packet_line, session_buffer = session_buffer.split("\n", 1)
                    if packet_line.strip():
                        # Đẩy gói tin vào hàng đợi cho Worker xử lý, không ghi trực tiếp ở đây
                        self.data_queue.put(packet_line)
        except Exception as ex:
            logger.error(f"Lỗi đọc gói tin: {ex}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _disk_writer_worker(self):
        """Luồng chạy ngầm lấy dữ liệu từ Queue và ghi xuống ổ đĩa an toàn."""
        unsaved_count = 0
        batch_data = []

        while self.is_running or not self.data_queue.empty():
            try:
                # Đợi tối đa 1s để lấy data, giúp vòng lặp liên tục kiểm tra self.is_running
                raw_str = self.data_queue.get(timeout=1.0)
                
                envelope = json.loads(raw_str)
                if envelope.get("type") == MessageType.DETECTION_RESULT:
                    payload_data = envelope.get("data", {})
                    video_name = payload_data.get("video_name", "unknown")
                    video_meta = payload_data.get("video_meta", {})

                    # Lưu vào RAM để tổng hợp báo cáo (Summary)
                    self.records_by_video[video_name].append(payload_data)
                    if video_meta and video_name not in self.video_meta_by_name:
                        self.video_meta_by_name[video_name] = video_meta

                    batch_data.append(payload_data)
                    unsaved_count += 1

                    logger.info(
                        f"[{video_name}] Frame #{payload_data.get('frame_number')} - "
                        f"{payload_data.get('person_count')} người"
                    )

                    # Lưu xuống đĩa khi đủ số lượng batch
                    if unsaved_count >= self.flush_batch_size:
                        self._flush_to_disk(batch_data)
                        batch_data.clear()
                        unsaved_count = 0
                
                self.data_queue.task_done()

            except queue.Empty:
                continue
            except Exception as err:
                logger.error(f"Lỗi định dạng/lưu trữ dữ liệu trong worker: {err}")

        # Khi server dừng, flush toàn bộ dữ liệu còn lại trong batch
        if batch_data:
            self._flush_to_disk(batch_data)

    def _flush_to_disk(self, batch_data):
        """Ghi batch hiện tại và xuất các file báo cáo."""
        try:
            # 1. Ghi nối (Append) log để tránh O(N^2)
            with open(Config.STORAGE_FILE, "a", encoding="utf-8") as writer:
                for record in batch_data:
                    writer.write(json.dumps(record, ensure_ascii=False) + "\n")

            # 2. Xử lý xuất file chi tiết & báo cáo cho từng video
            overall_videos = {}
            total_videos = 0
            total_frames = 0
            total_persons = 0

            for video_name, records in self.records_by_video.items():
                video_dir = os.path.join(Config.RESULTS_DIR, video_name)
                os.makedirs(video_dir, exist_ok=True)

                records_sorted = sorted(records, key=lambda x: x.get("frame_number", 0))

                frame_path = os.path.join(video_dir, "frame_detections.json")
                with open(frame_path, "w", encoding="utf-8") as f:
                    json.dump(records_sorted, f, indent=2, ensure_ascii=False)

                summary = self._build_video_summary(video_name, records_sorted)
                summary_path = os.path.join(video_dir, "summary.json")
                with open(summary_path, "w", encoding="utf-8") as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False)

                report_path = os.path.join(video_dir, "report.txt")
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(self._build_report_text(summary))

                detection_summary = summary["detection_summary"]
                overall_videos[video_name] = {
                    "frames": detection_summary["frames_processed"],
                    "total_persons": detection_summary["total_detections"],
                    "avg_persons_per_frame": detection_summary["avg_persons_per_frame"],
                    "result_dir": os.path.join("results", video_name),
                }

                total_videos += 1
                total_frames += detection_summary["frames_processed"]
                total_persons += detection_summary["total_detections"]

            # 3. Xuất file tổng hợp mọi video
            overall_summary = {
                "processing_info": {
                    "total_videos": total_videos,
                    "total_frames": total_frames,
                    "total_persons": total_persons,
                    "avg_persons_per_frame_overall": round(total_persons / total_frames, 2) if total_frames > 0 else 0,
                    "timestamp": datetime.now().isoformat(),
                },
                "videos": overall_videos,
            }

            overall_path = os.path.join(Config.RESULTS_DIR, "overall_summary.json")
            with open(overall_path, "w", encoding="utf-8") as f:
                json.dump(overall_summary, f, indent=2, ensure_ascii=False)

            logger.info("Đã flush dữ liệu xuống disk thành công.")

        except Exception as e:
            logger.error(f"Gặp sự cố khi lưu file: {e}")

    def _build_video_summary(self, video_name, records):
        """Tạo summary.json cho từng video."""
        records_sorted = sorted(records, key=lambda x: x.get("frame_number", 0))
        total_frames = len(records_sorted)
        total_persons = sum(item.get("person_count", 0) for item in records_sorted)
        max_persons = max((item.get("person_count", 0) for item in records_sorted), default=0)
        frames_with_persons = sum(1 for item in records_sorted if item.get("person_count", 0) > 0)
        avg_persons = round(total_persons / total_frames, 2) if total_frames > 0 else 0
        avg_processing_time = round(
            sum(item.get("processing_time_ms", 0) for item in records_sorted) / total_frames, 2
        ) if total_frames > 0 else 0
        detection_method = records_sorted[0].get("detection_method", "YOLO") if records_sorted else "YOLO"
        video_meta = self.video_meta_by_name.get(video_name, {})

        video_path = video_meta.get("source_path", "")
        fps = video_meta.get("fps", 0)
        width = video_meta.get("width")
        height = video_meta.get("height")
        total_source_frames = video_meta.get("total_frames")

        duration_seconds = None
        if total_source_frames and fps:
            try:
                duration_seconds = round(float(total_source_frames) / float(fps), 2)
            except Exception:
                duration_seconds = None

        return {
            "video_name": video_name,
            "video_path": video_path,
            "video_info": {
                "total_frames": total_source_frames if total_source_frames is not None else total_frames,
                "fps": fps,
                "resolution": f"{width}x{height}" if width and height else "",
                "duration_seconds": duration_seconds if duration_seconds is not None else "",
            },
            "detection_summary": {
                "total_detections": total_persons,
                "frames_processed": total_frames,
                "avg_persons_per_frame": avg_persons,
                "max_persons_in_frame": max_persons,
                "frames_with_persons": frames_with_persons,
                "detection_method": detection_method,
            },
            "processing": {
                "processing_time_seconds": round(
                    sum(item.get("processing_time_ms", 0) for item in records_sorted) / 1000.0, 2
                ),
                "avg_processing_time_ms": avg_processing_time,
                "timestamp": datetime.now().isoformat(),
            },
        }

    def _build_report_text(self, summary):
        d = summary["detection_summary"]
        info = summary["video_info"]
        proc = summary["processing"]

        return (
            f"Video Analysis Report: {summary['video_name']}\n"
            + "=" * 50 + "\n"
            + "Video Info:\n"
            + f"  - Source path: {summary['video_path']}\n"
            + f"  - Resolution: {info.get('resolution', '')}\n"
            + f"  - FPS: {info.get('fps', '')}\n"
            + f"  - Total Frames: {info.get('total_frames', '')}\n"
            + f"  - Duration: {info.get('duration_seconds', '')}\n\n"
            + "Detection Results:\n"
            + f"  - Detection Method: {d.get('detection_method', '')}\n"
            + f"  - Total Persons Detected: {d.get('total_detections', 0)}\n"
            + f"  - Average Persons/Frame: {d.get('avg_persons_per_frame', 0)}\n"
            + f"  - Max Persons in Single Frame: {d.get('max_persons_in_frame', 0)}\n"
            + f"  - Frames with Persons: {d.get('frames_with_persons', 0)}/{d.get('frames_processed', 0)}\n\n"
            + "Processing:\n"
            + f"  - Processing Time: {proc.get('processing_time_seconds', 0)} seconds\n"
            + f"  - Avg Processing Time: {proc.get('avg_processing_time_ms', 0)} ms\n"
        )

    def fetch_summary_stats(self):
        """Thống kê nhanh."""
        total_frames = sum(len(v) for v in self.records_by_video.values())
        total_persons = sum(
            item.get("person_count", 0)
            for records in self.records_by_video.values()
            for item in records
        )

        return {
            "total_videos": len(self.records_by_video),
            "total_frames": total_frames,
            "total_persons": total_persons,
            "avg_persons_per_frame": round(total_persons / total_frames, 2) if total_frames > 0 else 0,
        }

    def stop(self):
        """Dừng server và đợi ghi dữ liệu còn lại."""
        logger.info("Đang dừng Storage Server và chờ lưu dữ liệu còn lại...")
        self.is_running = False

        if self.writer_thread and self.writer_thread.is_alive():
            # Chờ worker xử lý xong queue hiện tại rồi thoát
            self.writer_thread.join(timeout=3.0) 

        summary = self.fetch_summary_stats()
        logger.info(f"Thống kê trước khi dừng: {summary}")

        if self.listener_socket:
            try:
                self.listener_socket.close()
            except Exception:
                pass

        logger.info("Storage Server đã dừng hoàn toàn.")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Dịch vụ lưu trữ dữ liệu phân tích tập trung.")
    parser.add_argument("--host", default=Config.HOST, help="Địa chỉ bind")
    parser.add_argument("--port", "-p", type=int, default=Config.STORAGE_PORT, help="Cổng chạy dịch vụ")
    parser.add_argument("--flush-batch", type=int, default=10, help="Số bản ghi trước khi flush xuống đĩa")

    args = parser.parse_args()
    app_storage = StorageServer(host=args.host, port=args.port, flush_batch_size=args.flush_batch)

    try:
        app_storage.start()
    except KeyboardInterrupt:
        app_storage.stop()


if __name__ == "__main__":
    main()