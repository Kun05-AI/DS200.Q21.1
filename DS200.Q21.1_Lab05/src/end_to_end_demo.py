import subprocess
import time
import sys
import os
import signal
import argparse
from datetime import datetime
from threading import Thread

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from settings import Config


class DemoRunner:
    """Chạy E2E demo: storage -> detector -> receiver -> sender."""

    def __init__(self, num_frames=10, video_path=None):
        self.limit_frames = num_frames
        self.path_video = video_path
        self.active_processes = []
        self.current_directory = os.path.dirname(os.path.abspath(__file__))
        self.root_directory = os.path.dirname(self.current_directory)

    def display_welcome_banner(self):
        print()
        print("*" * 70)
        print("  HỆ THỐNG PHÂN TÍCH LUỒNG NGƯỜI ĐI BỘ TRONG THỜI GIAN THỰC - DEMO")
        print("*" * 70)
        print("  Môn học: DS200.Q21.1 - Thực hành Phân tích Dữ liệu lớn")
        print("  Mã số sinh viên: 23521143")
        print(f"  Thời gian khởi tạo: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("*" * 70)

    def trigger_sub_server(self, script_name: str, parameters: list = None) -> subprocess.Popen:
        """Khởi động một tiến trình con."""
        target_path = os.path.join(self.current_directory, script_name)
        cmd_args = [sys.executable, target_path]
        if parameters:
            cmd_args.extend(parameters)

        logger_name = script_name.split(".")[0].upper()
        print(f"[Hệ thống] Đang khởi động: {logger_name}...")

        proc = subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=self.root_directory,
        )

        def log_router(pipe_stream, name):
            for data_line in iter(pipe_stream.readline, ""):
                if data_line:
                    print(f"[{name}] {data_line.rstrip()}")

        Thread(target=log_router, args=(proc.stdout, logger_name), daemon=True).start()
        Thread(target=log_router, args=(proc.stderr, logger_name + "-ERR"), daemon=True).start()

        return proc

    def execute_demo(self):
        """Khởi động chuỗi server và chạy sender."""
        self.display_welcome_banner()

        try:
            storage_p = self.trigger_sub_server("storage_server.py", ["--flush-batch", "10"])
            self.active_processes.append(storage_p)
            time.sleep(2)

            detector_p = self.trigger_sub_server("detect_object.py", ["--no-spark", "--no-sahi"])
            self.active_processes.append(detector_p)
            time.sleep(2)

            receiver_p = self.trigger_sub_server("receiver.py")
            self.active_processes.append(receiver_p)
            time.sleep(2)

            sender_params = ["--frames", str(self.limit_frames), "--fps", "2"]
            if self.path_video:
                sender_params.extend(["--video", self.path_video])

            sender_p = self.trigger_sub_server("sender.py", sender_params)
            self.active_processes.append(sender_p)

            print("\n[Hệ thống] Pipeline đang chạy...")
            sender_p.wait()
            time.sleep(2)
            print("\n[Hệ thống] Demo hoàn tất.")

        except Exception as e:
            print(f"[Lỗi demo] {e}")
        finally:
            self.shutdown_pipeline()

    def shutdown_pipeline(self):
        """Tắt tất cả tiến trình con."""
        print("\n[Hệ thống] Đang dừng các server...")
        for p in self.active_processes:
            try:
                p.terminate()
                p.wait(timeout=2)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
        print("[Hệ thống] Đã dừng toàn bộ tiến trình.")


def quick_verification():
    """Kiểm tra nhanh thư viện."""
    print("--- KIỂM TRA ĐỘ KHẢ DỤNG HỆ THỐNG ---")
    dependencies = [
        ("cv2", "OpenCV"),
        ("mediapipe", "MediaPipe"),
        ("ultralytics", "YOLO"),
    ]
    for module_pkg, printable_name in dependencies:
        try:
            __import__(module_pkg)
            print(f"  [✓] {printable_name} đã sẵn sàng.")
        except ImportError:
            print(f"  [!] Thiếu {printable_name}.")


def main():
    parser = argparse.ArgumentParser(description="Điều phối demo end-to-end.")
    parser.add_argument("--frames", "-n", type=int, default=10, help="Số frame gửi thử nghiệm")
    parser.add_argument("--video", "-v", help="Đường dẫn file video")
    parser.add_argument("--test", "-t", action="store_true", help="Chỉ kiểm tra thư viện")

    args = parser.parse_args()
    if args.test:
        quick_verification()
        return

    video_abs_path = os.path.abspath(args.video) if args.video else None
    runner = DemoRunner(num_frames=args.frames, video_path=video_abs_path)

    def exit_gracefully(signum, frame):
        runner.shutdown_pipeline()
        sys.exit(0)

    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    runner.execute_demo()


if __name__ == "__main__":
    main()