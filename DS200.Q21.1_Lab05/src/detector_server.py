import socket
import json
import logging
import threading
import base64
import time
import argparse
import os
import traceback
from datetime import datetime

try:
    import torch
except ImportError:
    pass

try:
    import cv2
    import numpy as np
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False
    print("[Hệ thống] Cảnh báo: OpenCV chưa được cài đặt. Không thể xử lý ảnh.")

try:
    from ultralytics import YOLO
    HAS_YOLO = True
except ImportError:
    HAS_YOLO = False
    print("[Hệ thống] Cảnh báo: Lỗi nạp module Ultralytics YOLO.")

try:
    from sahi import AutoDetectionModel
    from sahi.predict import get_sliced_prediction
    HAS_SAHI = True
except ImportError:
    HAS_SAHI = False
    print("[Hệ thống] Thông tin: Tính năng cắt ảnh SAHI đang tắt do thiếu thư viện.")

from settings import Config, MessageType

logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
sys_logger = logging.getLogger("VisionInferenceNode")

DEFAULT_SNAPSHOT_INTERVAL = 50

# ✅ CUSTOM COLOR CONSTANTS
COLOR_SAHI = (0, 0, 255)        # ĐỎ cho SAHI
COLOR_YOLO = (255, 0, 0)        # XANH DƯƠNG cho YOLO
BOX_THICKNESS = 5
TEXT_FONT_SCALE = 1.2
TEXT_THICKNESS = 2

# ✅ NMS PARAMETERS
NMS_THRESHOLD = 0.5             # Loại bỏ BBX chồng > 50%
CONFIDENCE_THRESHOLD_MIN = 0.3  # Confidence tối thiểu

def compute_iou(box1, box2):
    """✅ Tính IoU giữa 2 bounding box"""
    x1_min, y1_min, w1, h1 = box1
    x1_max, y1_max = x1_min + w1, y1_min + h1
    
    x2_min, y2_min, w2, h2 = box2
    x2_max, y2_max = x2_min + w2, y2_min + h2
    
    # Tính giao (intersection)
    xi_min = max(x1_min, x2_min)
    yi_min = max(y1_min, y2_min)
    xi_max = min(x1_max, x2_max)
    yi_max = min(y1_max, y2_max)
    
    if xi_max <= xi_min or yi_max <= yi_min:
        return 0.0
    
    inter_area = (xi_max - xi_min) * (yi_max - yi_min)
    
    # Tính hợp (union)
    box1_area = w1 * h1
    box2_area = w2 * h2
    union_area = box1_area + box2_area - inter_area
    
    iou = inter_area / union_area if union_area > 0 else 0
    return iou

def apply_nms(detections, nms_threshold=NMS_THRESHOLD):
    """✅ NMS: Loại bỏ BBX chồng lặp"""
    if not detections:
        return detections
    
    # Sắp xếp theo confidence (cao → thấp)
    sorted_dets = sorted(detections, key=lambda x: x["confidence"], reverse=True)
    
    kept_boxes = []
    
    for i, det in enumerate(sorted_dets):
        is_overlapped = False
        
        for kept_det in kept_boxes:
            iou = compute_iou(
                (det["x"], det["y"], det["width"], det["height"]),
                (kept_det["x"], kept_det["y"], kept_det["width"], kept_det["height"])
            )
            
            # Nếu chồng lấp > threshold → loại bỏ
            if iou > nms_threshold:
                is_overlapped = True
                break
        
        if not is_overlapped:
            kept_boxes.append(det)
    
    return kept_boxes

class AIInferenceService:
    """Nút dịch vụ AI trung tâm - CÓ NMS FIX LỖI ĐẾM"""

    def __init__(
        self,
        listen_ip=Config.HOST,
        listen_port=Config.PROCESSING_PORT,
        spark_enabled=False,
        sahi_enabled=True,
        compute_unit="cpu",
        snapshot_freq: int = DEFAULT_SNAPSHOT_INTERVAL,
    ):
        self.ip_address = listen_ip
        self.port_number = listen_port
        self.use_spark = spark_enabled
        self.use_slicer = sahi_enabled and HAS_SAHI

        self.net_socket = None
        self.db_connection = None
        self.db_mutex = threading.Lock()

        self.service_running = False
        self.processed_frames = 0
        self.counter_mutex = threading.Lock()

        self.primary_net = None
        self.slicer_net = None
        self.hw_device = compute_unit if compute_unit in ("cpu", "cuda") else "cpu"

        self.snapshot_interval = max(1, snapshot_freq)
        self.dir_results = Config.OUTPUT_DIR
        self.dir_visuals = Config.SCREENSHOTS_DIR
        os.makedirs(self.dir_results, exist_ok=True)
        os.makedirs(self.dir_visuals, exist_ok=True)

        self.video_metrics = {}
        self.metrics_mutex = threading.Lock()

        self._boot_neural_networks()

    def _boot_neural_networks(self):
        """Khởi động và nạp các trọng số học sâu vào bộ nhớ."""
        if not HAS_YOLO:
            return

        weight_file = Config.YOLO_MODEL_PATH
        
        try:
            self.primary_net = YOLO(weight_file)
            
            if self.hw_device == "cuda":
                self.primary_net.to("cuda")
                sys_logger.info("Bộ máy YOLO đang tăng tốc qua GPU (CUDA).")
            else:
                sys_logger.info("Bộ máy YOLO đang chạy trên CPU tiêu chuẩn.")

            sys_logger.info(f"Hoàn tất nạp mô hình nhận diện từ: {weight_file}")
        except Exception as err:
            sys_logger.error(f"Sự cố lúc khởi tạo nhân YOLO: {err}")
            self.primary_net = None

        if self.use_slicer and HAS_SAHI and self.primary_net:
            try:
                self.slicer_net = AutoDetectionModel.from_pretrained(
                    model_type="ultralytics",
                    model_path=weight_file,
                    confidence_threshold=Config.CONFIDENCE_THRESHOLD,
                    device=self.hw_device,
                )
                sys_logger.info("Tích hợp thành công module cắt ảnh lưới SAHI + NMS FIX.")
            except Exception as err:
                sys_logger.warning(f"Bỏ qua SAHI do gặp lỗi khởi tạo: {err}")
                self.slicer_net = None

    def launch(self):
        """Mở cổng mạng, chờ luồng dữ liệu từ trạm thu (Receiver)."""
        self.net_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.net_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.net_socket.bind((self.ip_address, self.port_number))
        self.net_socket.listen(5)
        self.service_running = True

        sys_logger.info("*" * 65)
        sys_logger.info(f"DỊCH VỤ NHẬN DIỆN AI ĐÃ MỞ TẠI {self.ip_address}:{self.port_number}")
        sys_logger.info(f"Spark: {'ACTIVE' if self.use_spark else 'OFF'} | SAHI: {'ACTIVE' if self.use_slicer else 'OFF'} | Thiết bị: {self.hw_device.upper()}")
        sys_logger.info(f"✅ NMS: ENABLED (NMS_THRESHOLD={NMS_THRESHOLD}) - Loại bỏ BBX chồng lặp")
        sys_logger.info(f"BBX Colors: SAHI=Red, YOLO=Blue | Thickness={BOX_THICKNESS} | TextScale={TEXT_FONT_SCALE}")
        sys_logger.info("*" * 65)

        self._establish_db_link()

        while self.service_running:
            try:
                income_conn, income_addr = self.net_socket.accept()
                sys_logger.info(f"Chấp nhận luồng cung cấp hình ảnh từ {income_addr}")
                worker = threading.Thread(target=self._read_network_stream, args=(income_conn,), daemon=True)
                worker.start()
            except Exception as e:
                if self.service_running:
                    sys_logger.error(f"Lỗi cổng giao tiếp đầu vào: {e}")

    def _establish_db_link(self, max_attempts: int = 3, retry_wait: float = 1.0):
        """Quản lý kết nối TCP đến hệ thống Storage."""
        with self.db_mutex:
            if self.db_connection:
                try: self.db_connection.close()
                except: pass
                self.db_connection = None

            for try_idx in range(1, max_attempts + 1):
                try:
                    tcp_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tcp_client.connect((Config.HOST, Config.STORAGE_PORT))
                    self.db_connection = tcp_client
                    sys_logger.info("Móc nối tới máy chủ kho dữ liệu thành công.")
                    return True
                except Exception as ex:
                    sys_logger.warning(f"Lỗi nối tới Storage (Thử lần {try_idx}/{max_attempts}): {ex}")
                    try: tcp_client.close()
                    except: pass
                    self.db_connection = None
                    
                    if try_idx < max_attempts:
                        time.sleep(retry_wait)

            sys_logger.warning("Đã cạn số lần thử kết nối Storage. Hệ thống sẽ tiếp tục thử lại sau.")
            return False

    def _read_network_stream(self, client_socket):
        """Hút dữ liệu từ socket, bóc tách các dòng JSON rời rạc."""
        text_buffer = ""

        try:
            while self.service_running:
                data_chunk = client_socket.recv(Config.BUFFER_SIZE)
                if not data_chunk:
                    break

                text_buffer += data_chunk.decode("utf-8", errors="ignore")

                while "\n" in text_buffer:
                    json_str, text_buffer = text_buffer.split("\n", 1)
                    if json_str.strip():
                        self._analyze_payload(json_str)
        except Exception as ex:
            sys_logger.error(f"Đứt gãy trong quá trình đọc stream: {ex}")
        finally:
            try: client_socket.close()
            except: pass

    def _analyze_payload(self, raw_json):
        """Giải nén gói tin, decode ảnh, quét AI và chuyển tiếp kết quả."""
        try:
            packet = json.loads(raw_json)

            if packet.get("type") not in (MessageType.FRAME, "FRAME"):
                return

            inner_data = packet.get("payload", {})
            b64_string = inner_data.get("image_data", "")
            
            try:
                frame_seq = int(inner_data.get("frame_id", 0))
            except ValueError:
                frame_seq = 0
                
            v_name = inner_data.get("video_name", "unknown")
            v_metadata = inner_data.get("video_meta", {})

            if not HAS_CV2 or not b64_string:
                return

            tick_start = time.perf_counter()
            
            try:
                img_bytes = base64.b64decode(b64_string)
                np_array = np.frombuffer(img_bytes, dtype=np.uint8)
                bgr_image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)
            except Exception as e:
                sys_logger.error(f"Hỏng cấu trúc mã hóa Base64: {e}")
                return

            if bgr_image is None:
                return

            found_entities = self._run_object_detection(bgr_image)
            
            # ✅ APPLY NMS - Loại bỏ BBX chồng lặp
            found_entities = apply_nms(found_entities, nms_threshold=NMS_THRESHOLD)
            
            compute_time_ms = (time.perf_counter() - tick_start) * 1000.0
            
            with self.counter_mutex:
                self.processed_frames += 1

            algo_used = "SAHI + YOLO" if (self.use_slicer and self.slicer_net) else "YOLO"

            outbound_data = {
                "video_name": v_name,
                "video_meta": v_metadata,
                "frame_number": frame_seq,
                "timestamp": inner_data.get("timestamp", time.time()),
                "person_count": len(found_entities),  # ✅ Đếm ĐÚNG rồi (sau NMS)
                "bounding_boxes": found_entities,
                "processing_time_ms": round(compute_time_ms, 2),
                "detection_method": algo_used,
            }

            visual_mode = "with-sahi" if (self.use_slicer and self.slicer_net) else "no-sahi"
            self._export_visual_evidence(
                matrix_img=bgr_image,
                targets=found_entities,
                seq_id=frame_seq,
                folder_mode=visual_mode,
                vid_title=v_name,
                interval=self.snapshot_interval,
                use_sahi=(self.use_slicer and self.slicer_net),
            )

            self._log_internal_stats(v_name, outbound_data)
            self._push_to_database(outbound_data)

        except Exception as e:
            sys_logger.error(f"Khủng hoảng trong pipeline phân tích ảnh: {e}")
            sys_logger.debug(traceback.format_exc())

    def _run_object_detection(self, bgr_image):
        """Thực thi mạng nơ-ron: Ưu tiên SAHI, dự phòng YOLO thuần."""
        results_list = []
        sahi_success = False

        if self.use_slicer and self.slicer_net:
            try:
                slicing_output = get_sliced_prediction(
                    bgr_image,
                    self.slicer_net,
                    slice_height=256,
                    slice_width=256,
                    overlap_height_ratio=0.2,
                    overlap_width_ratio=0.2,
                )

                for item in slicing_output.object_prediction_list:
                    category_info = getattr(item, "category", None)
                    if not category_info:
                        continue
                        
                    if category_info.name == "person" or category_info.id == 0:
                        box_data = item.bbox
                        certainty = 0.0

                        try: 
                            certainty = float(item.score.value)
                        except (AttributeError, TypeError, ValueError): 
                            pass

                        try:
                            left, top, right, bottom = box_data.to_xyxy()
                        except:
                            left, top, right, bottom = box_data.minx, box_data.miny, box_data.maxx, box_data.maxy

                        results_list.append({
                            "x": int(left),
                            "y": int(top),
                            "width": int(right - left),
                            "height": int(bottom - top),
                            "confidence": round(certainty, 3),
                        })
                sahi_success = True
            except Exception as e:
                sys_logger.warning(f"SAHI gãy luồng, ép buộc chuyển sang YOLO: {e}")
                sahi_success = False

        if not sahi_success and self.primary_net:
            try:
                net_preds = self.primary_net(
                    bgr_image,
                    verbose=False,
                    device=self.hw_device
                )[0]
                
                boxes = getattr(net_preds, "boxes", None)
                if boxes is None or len(boxes) == 0:
                    sys_logger.debug("YOLO: Không phát hiện được đối tượng nào.")
                    return results_list
                
                for box in boxes:
                    try:
                        obj_class = int(box.cls[0])
                        obj_conf = float(box.conf[0])
                    except (IndexError, TypeError, ValueError, AttributeError):
                        continue

                    # ✅ Chỉ lấy class 0 (person) và confidence cao
                    if obj_class == 0 and obj_conf >= CONFIDENCE_THRESHOLD_MIN:
                        try:
                            xyxy = box.xyxy[0]
                            cords = xyxy.tolist()
                            
                            results_list.append({
                                "x": int(cords[0]),
                                "y": int(cords[1]),
                                "width": int(cords[2] - cords[0]),
                                "height": int(cords[3] - cords[1]),
                                "confidence": round(obj_conf, 3),
                            })
                        except (IndexError, AttributeError, TypeError):
                            continue
                            
            except Exception as e:
                sys_logger.error(f"Lỗi tàn phá tầng suy luận YOLO: {e}")
                sys_logger.debug(traceback.format_exc())

        return results_list

    def _export_visual_evidence(self, matrix_img, targets, seq_id, folder_mode, vid_title, interval, use_sahi=False):
        """Vẽ BBX với 2 màu + NMS đã loại bỏ chồng lặp"""
        try:
            if interval and (seq_id % interval) != 0:
                return

            save_path_dir = os.path.join(self.dir_visuals, folder_mode, vid_title)
            os.makedirs(save_path_dir, exist_ok=True)
            
            canvas = matrix_img.copy()
            
            if use_sahi:
                box_color = COLOR_SAHI
                label_prefix = "SAHI"
            else:
                box_color = COLOR_YOLO
                label_prefix = "YOLO"

            for t in targets:
                try:
                    px = int(t.get("x", 0))
                    py = int(t.get("y", 0))
                    pw = int(t.get("width", 0))
                    ph = int(t.get("height", 0))
                    pconf = float(t.get("confidence", 0.0))

                    cv2.rectangle(canvas, (px, py), (px + pw, py + ph), box_color, BOX_THICKNESS)

                    text_tag = f"{label_prefix} {pconf:.2f}"
                    (txt_w, txt_h), _ = cv2.getTextSize(
                        text_tag, 
                        cv2.FONT_HERSHEY_SIMPLEX, 
                        TEXT_FONT_SCALE,
                        TEXT_THICKNESS
                    )

                    padding = 10
                    cv2.rectangle(
                        canvas, 
                        (px - padding, max(0, py - txt_h - padding - 5)), 
                        (px + txt_w + padding, py), 
                        box_color, 
                        -1
                    )
                    
                    cv2.putText(
                        canvas, 
                        text_tag, 
                        (px + 5, py - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 
                        TEXT_FONT_SCALE,
                        (0, 0, 0),
                        TEXT_THICKNESS
                    )
                except (TypeError, ValueError, KeyError):
                    continue

            file_string = f"frame_{seq_id:04d}.jpg"
            target_file = os.path.join(save_path_dir, file_string)
            cv2.imwrite(target_file, canvas)

        except Exception as e:
            sys_logger.warning(f"Thất bại trong việc lưu ảnh minh chứng: {e}")

    def _log_internal_stats(self, vid_key, stat_record):
        """Đẩy lịch sử chạy vào bộ nhớ RAM để xuất báo cáo sau cùng."""
        with self.metrics_mutex:
            if vid_key not in self.video_metrics:
                self.video_metrics[vid_key] = []
            self.video_metrics[vid_key].append(stat_record)

    def _push_to_database(self, payload_dict):
        """Niêm phong dữ liệu JSON và bắn qua Storage."""
        msg_pack = {
            "type": MessageType.DETECTION_RESULT,
            "data": payload_dict,
            "timestamp": datetime.now().isoformat(),
        }
        encoded_line = (json.dumps(msg_pack) + "\n").encode("utf-8")

        with self.db_mutex:
            if not self.db_connection:
                if not self._establish_db_link():
                    return

            try:
                self.db_connection.sendall(encoded_line)
            except Exception as e:
                sys_logger.error(f"Giao tiếp Storage đứt gãy giữa chừng: {e}")
                try: self.db_connection.close()
                except: pass
                self.db_connection = None

    def close_service(self):
        """Chấm dứt toàn bộ luồng, in báo cáo tổng kết."""
        self.service_running = False

        if self.net_socket:
            try: self.net_socket.close()
            except: pass

        with self.db_mutex:
            if self.db_connection:
                try: self.db_connection.close()
                except: pass
                self.db_connection = None

        with self.metrics_mutex:
            sys_logger.info("=== BÁO CÁO CỤC BỘ ===")
            for v_name, metrics in self.video_metrics.items():
                t_frames = len(metrics)
                t_humans = sum(r.get("person_count", 0) for r in metrics)
                avg = round((t_humans / t_frames) if t_frames > 0 else 0, 2)
                sys_logger.info(f"-> Nguồn: {v_name} | Số khung: {t_frames} | Tổng phát hiện: {t_humans} | Trung bình: {avg} người/khung")

        with self.counter_mutex:
            sys_logger.info(f"Dịch vụ AI đã ngắt. Tổng sản lượng tiêu thụ: {self.processed_frames} khung hình.")


def main():
    cli = argparse.ArgumentParser(description="Core AI Server nhận diện người kết hợp Deep Learning (YOLO & SAHI).")
    cli.add_argument("--host", default=Config.HOST, help="IP Ràng buộc")
    cli.add_argument("--port", "-p", type=int, default=Config.PROCESSING_PORT, help="Port tiếp nhận")
    cli.add_argument("--no-spark", action="store_true", help="Vô hiệu hóa bộ phân tải Spark")
    cli.add_argument("--no-sahi", action="store_true", help="Vô hiệu hóa module chia cắt ảnh")
    cli.add_argument("--device", choices=["cpu", "cuda"], default="cpu", help="Bộ xử lý Inference (cpu/cuda)")
    cli.add_argument("--save-every", type=int, default=DEFAULT_SNAPSHOT_INTERVAL, help="Tần suất bóp cò lưu ảnh mẫu")

    params = cli.parse_args()

    service_node = AIInferenceService(
        listen_ip=params.host,
        listen_port=params.port,
        spark_enabled=not params.no_spark,
        sahi_enabled=not params.no_sahi,
        compute_unit=params.device,
        snapshot_freq=params.save_every,
    )

    try:
        service_node.launch()
    except KeyboardInterrupt:
        service_node.close_service()

if __name__ == "__main__":
    main()