import socket
import json
import logging
import time
import base64
import argparse
from datetime import datetime
import os

try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    print("[Cảnh báo] Không tìm thấy OpenCV. Sender cần OpenCV để đọc video/camera.")

from settings import Config

logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("VideoSender")


class FrameSender:
    """Đọc frame từ video/camera và gửi qua TCP dưới dạng JSON line."""

    def __init__(
        self,
        source=None,
        fps=2,
        target_host=Config.HOST,
        target_port=Config.RECEIVER_PORT,
        max_frames=None,
        video_name=None,
    ):
        self.video_source = source
        self.frame_rate = fps
        self.server_ip = target_host
        self.server_port = target_port
        self.video_capture = None
        self.network_socket = None
        self.is_active = False
        self.max_frames = max_frames

        self.video_name = video_name or (
            os.path.splitext(os.path.basename(source))[0] if isinstance(source, str) else "camera"
        )

        self.video_meta = {
            "source_type": "unknown",
            "source_path": None,
            "fps": fps,
            "width": None,
            "height": None,
            "total_frames": None,
        }

        if source is not None:
            self._setup_video_stream()

    def _setup_video_stream(self):
        """Khởi tạo nguồn video/camera."""
        if not CV2_AVAILABLE:
            logger.error("OpenCV chưa cài đặt, không thể mở nguồn video/camera.")
            return

        self.video_capture = cv2.VideoCapture(self.video_source)
        if not self.video_capture.isOpened():
            logger.error(f"Không thể mở nguồn video/camera: {self.video_source}")
            self.video_capture = None
            return

        if isinstance(self.video_source, int):
            self.video_meta["source_type"] = "camera"
            self.video_meta["source_path"] = str(self.video_source)
        else:
            self.video_meta["source_type"] = "video"
            self.video_meta["source_path"] = os.path.abspath(str(self.video_source))
            self.video_meta["total_frames"] = int(self.video_capture.get(cv2.CAP_PROP_FRAME_COUNT)) or None
            cap_fps = float(self.video_capture.get(cv2.CAP_PROP_FPS))
            if cap_fps > 0:
                self.frame_rate = cap_fps
                self.video_meta["fps"] = cap_fps

        width = int(self.video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(self.video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self.video_meta["width"] = width if width > 0 else None
        self.video_meta["height"] = height if height > 0 else None

    def _acquire_frame_data(self):
        """Lấy frame và mã hóa base64."""
        if not CV2_AVAILABLE or not self.video_capture or not self.video_capture.isOpened():
            return False, ""

        ret, frame = self.video_capture.read()
        if not ret:
            return False, ""

        ok, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if not ok:
            logger.error("Không thể mã hóa frame sang JPEG.")
            return False, ""

        b64_data = base64.b64encode(buffer).decode("utf-8")
        return True, b64_data

    def start(self):
        """Kết nối tới Receiver và bắt đầu gửi frame."""
        if not CV2_AVAILABLE:
            logger.error("Sender cần OpenCV để chạy.")
            return

        if self.video_source is None:
            logger.error("Chưa cung cấp nguồn video/camera.")
            return

        if self.video_capture is None:
            logger.error("Không có nguồn video hợp lệ.")
            return

        try:
            self.network_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.network_socket.connect((self.server_ip, self.server_port))
            self.is_active = True
            logger.info(f"Đã kết nối tới Receiver tại {self.server_ip}:{self.server_port}")

            interval = 1.0 / self.frame_rate if self.frame_rate and self.frame_rate > 0 else 0.5
            idx_frame = 0

            while self.is_active:
                start_loop = time.time()
                idx_frame += 1

                success, b64_str = self._acquire_frame_data()
                if not success:
                    logger.info("Hết frame hoặc không đọc được frame tiếp theo.")
                    break

                packet = {
                    "frame_id": idx_frame,
                    "timestamp": time.time(),
                    "image_data": b64_str,
                    "video_name": self.video_name,
                    "video_meta": self.video_meta,
                }

                serialized_data = (json.dumps({"type": "FRAME", "payload": packet}) + "\n").encode("utf-8")

                try:
                    self.network_socket.sendall(serialized_data)
                except BrokenPipeError:
                    logger.error("Kết nối tới Receiver bị đứt.")
                    break
                except Exception as err:
                    logger.error(f"Lỗi khi gửi dữ liệu: {err}")
                    break

                if self.max_frames is not None and idx_frame >= self.max_frames:
                    logger.info(f"Đã đạt giới hạn {self.max_frames} frame.")
                    break

                elapsed = time.time() - start_loop
                sleep_time = max(0.0, interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except Exception as err:
            logger.error(f"Lỗi trong quá trình Sender: {err}")
        finally:
            self.stop()

    def stop(self):
        """Giải phóng tài nguyên."""
        self.is_active = False

        if self.network_socket:
            try:
                self.network_socket.close()
            except Exception:
                pass

        if self.video_capture:
            try:
                self.video_capture.release()
            except Exception:
                pass

        logger.info("Sender đã dừng.")


def main():
    parser = argparse.ArgumentParser(description="Gửi dữ liệu khung hình camera/video qua TCP Socket.")
    parser.add_argument("--video", "-v", help="Đường dẫn file video")
    parser.add_argument("--camera", "-c", type=int, help="Chỉ số camera")
    parser.add_argument("--fps", "-f", type=float, default=2, help="Tốc độ khung hình gửi")
    parser.add_argument("--frames", "-n", type=int, help="Giới hạn số frame gửi")
    parser.add_argument("--host", default=Config.HOST, help="Địa chỉ IP của receiver")
    parser.add_argument("--port", "-p", type=int, default=Config.RECEIVER_PORT, help="Cổng receiver")
    parser.add_argument("--name", "-N", help="Tên video dùng cho output")

    args = parser.parse_args()

    src_input = None
    if args.video:
        src_input = args.video
    elif args.camera is not None:
        src_input = int(args.camera)

    if args.name:
        video_name = args.name
    elif args.video:
        video_name = os.path.splitext(os.path.basename(args.video))[0]
    elif args.camera is not None:
        video_name = f"camera_{args.camera}"
    else:
        video_name = "unknown"

    app_sender = FrameSender(
        source=src_input,
        fps=args.fps,
        target_host=args.host,
        target_port=args.port,
        max_frames=args.frames,
        video_name=video_name,
    )

    try:
        app_sender.start()
    except KeyboardInterrupt:
        app_sender.stop()


if __name__ == "__main__":
    main()