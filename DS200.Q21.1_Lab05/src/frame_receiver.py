import socket
import json
import logging
import threading
import argparse
from datetime import datetime

from settings import Config, MessageType

logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("NetworkReceiver")


class FrameReceiver:
    """Nhận JSON line từ Sender và chuyển tiếp sang Detector."""

    def __init__(self, host=Config.HOST, port=Config.RECEIVER_PORT):
        self.listen_host = host
        self.listen_port = port
        self.listener_socket = None

        self.detector_client = None
        self.detector_lock = threading.Lock()

        self.is_running = False
        self.received_counter = 0

    def start(self):
        """Khởi động server receiver."""
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind((self.listen_host, self.listen_port))
        self.listener_socket.listen(5)
        self.is_running = True

        logger.info("=" * 60)
        logger.info(f"RECEIVER đang chạy tại {self.listen_host}:{self.listen_port}")
        logger.info("=" * 60)

        self._establish_detector_link()

        while self.is_running:
            try:
                client_conn, client_addr = self.listener_socket.accept()
                logger.info(f"Sender đã kết nối từ: {client_addr}")
                worker = threading.Thread(target=self._manage_client_stream, args=(client_conn,), daemon=True)
                worker.start()
            except Exception as e:
                if self.is_running:
                    logger.error(f"Lỗi chấp nhận kết nối mới: {e}")

    def _establish_detector_link(self, retries: int = 3, delay: float = 1.0):
        """Kết nối tới Detector server."""
        with self.detector_lock:
            if self.detector_client:
                try:
                    self.detector_client.close()
                except Exception:
                    pass
                self.detector_client = None

            for attempt in range(1, retries + 1):
                try:
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client.connect((Config.HOST, Config.PROCESSING_PORT))
                    self.detector_client = client
                    logger.info("Đã kết nối tới Detector server.")
                    return True
                except Exception as ex:
                    logger.warning(f"Không kết nối được tới Detector (lần {attempt}/{retries}): {ex}")
                    try:
                        client.close()
                    except Exception:
                        pass
                    self.detector_client = None
                    if attempt < retries:
                        threading.Event().wait(delay)

            logger.warning("Chưa thể liên kết Detector server.")
            return False

    def _manage_client_stream(self, conn):
        """Đọc từng dòng JSON từ Sender."""
        stream_buffer = ""

        try:
            while self.is_running:
                data_bytes = conn.recv(Config.BUFFER_SIZE)
                if not data_bytes:
                    break

                stream_buffer += data_bytes.decode("utf-8", errors="ignore")

                while "\n" in stream_buffer:
                    line, stream_buffer = stream_buffer.split("\n", 1)
                    if line.strip():
                        self._dispatch_message(line)
        except Exception as ex:
            logger.error(f"Sự cố khi đọc stream từ Sender: {ex}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _dispatch_message(self, text_payload: str):
        """Parse JSON và forward sang detector."""
        try:
            parsed_object = json.loads(text_payload)
            self.received_counter += 1

            forward_packet = {
                "type": MessageType.FRAME,
                "seq": self.received_counter,
                "payload": parsed_object.get("payload", parsed_object),
                "timestamp": datetime.now().isoformat(),
            }

            output_str = json.dumps(forward_packet) + "\n"
            self._send_to_detector(output_str)

        except json.JSONDecodeError as err_json:
            logger.error(f"JSON lỗi từ Sender: {err_json}")
        except Exception as err_general:
            logger.error(f"Lỗi khi phân phối thông điệp: {err_general}")

    def _send_to_detector(self, output_str: str):
        """Gửi data sang detector với lock."""
        with self.detector_lock:
            if not self.detector_client:
                if not self._establish_detector_link():
                    return

            try:
                self.detector_client.sendall(output_str.encode("utf-8"))
            except Exception as err:
                logger.error(f"Không thể đẩy frame sang Detector: {err}")
                try:
                    self.detector_client.close()
                except Exception:
                    pass
                self.detector_client = None

    def stop(self):
        """Dừng server."""
        self.is_running = False

        if self.listener_socket:
            try:
                self.listener_socket.close()
            except Exception:
                pass

        with self.detector_lock:
            if self.detector_client:
                try:
                    self.detector_client.close()
                except Exception:
                    pass
                self.detector_client = None

        logger.info(f"Receiver đã dừng. Tổng frame nhận: {self.received_counter}")


def main():
    parser = argparse.ArgumentParser(description="TCP Receiver nhận luồng ảnh từ Sender.")
    parser.add_argument("--host", default=Config.HOST, help="Địa chỉ bind")
    parser.add_argument("--port", "-p", type=int, default=Config.RECEIVER_PORT, help="Cổng lắng nghe")

    args = parser.parse_args()
    server_receiver = FrameReceiver(host=args.host, port=args.port)

    try:
        server_receiver.start()
    except KeyboardInterrupt:
        server_receiver.stop()


if __name__ == "__main__":
    main()