import os
import sys
import cv2
import numpy as np
from pathlib import Path
from typing import Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from settings import Config

try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    print("[Hệ thống] Yêu cầu bắt buộc cài đặt gói ultralytics.")
    sys.exit(1)

try:
    from sahi import AutoDetectionModel
    from sahi.predict import get_sliced_prediction
    SAHI_AVAILABLE = True
except ImportError:
    SAHI_AVAILABLE = False
    print("[Cảnh báo] SAHI vắng mặt, hình ảnh so sánh sẽ sử dụng bản sao mô phỏng.")

class ComparisonGenerator:
    """Tập lệnh sinh ảnh trực quan đối chiếu hiệu năng khoanh vùng vật thể giữa YOLO và SAHI."""
    
    def __init__(self, model_path: str, confidence: float = 0.3):
        self.path_to_model = model_path
        self.conf_level = confidence
        self.standard_yolo = None
        self.sahi_wrapper = None
        self._load_core_nets()
        
    def _load_core_nets(self):
        """Khởi động bộ khung nhận diện học sâu."""
        if YOLO_AVAILABLE:
            self.standard_yolo = YOLO(self.path_to_model)
            if SAHI_AVAILABLE:
                self.sahi_wrapper = AutoDetectionModel.from_pretrained(
                    model_type="ultralytics",
                    model_path=self.path_to_model,
                    confidence_threshold=self.conf_level,
                    device="cpu"
                )

    def process_and_save_samples(self, video_file: str, out_root: str, sample_interval: int = 150):
        """Trích xuất ảnh mẫu tại các frame chỉ định và vẽ bounding box đối chiếu tương quan."""
        v_stem = Path(video_file).stem
        cap = cv2.VideoCapture(video_file)
        if not cap.isOpened():
            print(f"[Lỗi] Không thể mở video: {video_file}")
            return
            
        f_idx = 0
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            f_idx += 1
            
            if f_idx % sample_interval == 0 or f_idx == 10:
                img_yolo = frame.copy()
                img_sahi = frame.copy()
                
                # ---------------------------------------------------------
                # 1. Vẽ đồ họa nhận diện YOLO tiêu chuẩn
                # ---------------------------------------------------------
                yolo_results = self.standard_yolo(img_yolo, verbose=False)[0]
                yolo_count = 0
                
                boxes = yolo_results.boxes if yolo_results.boxes else []
                for box in boxes:
                    try:
                        if int(box.cls[0]) == 0 and float(box.conf[0]) >= self.conf_level:
                            yolo_count += 1
                            pts = box.xyxy[0].tolist()
                            cv2.rectangle(img_yolo, (int(pts[0]), int(pts[1])), (int(pts[2]), int(pts[3])), (0, 255, 0), 2)
                            cv2.putText(img_yolo, f"Person {float(box.conf[0]):.2f}", (int(pts[0]), int(pts[1]) - 6),
                                        cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 255, 0), 1)
                    except (IndexError, TypeError, AttributeError):
                        continue
                                    
                cv2.putText(img_yolo, f"Frame: {f_idx} | Total: {yolo_count}", (20, 40), 
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
                            
                # ---------------------------------------------------------
                # 2. Vẽ đồ họa nhận diện SAHI thông minh (ĐÃ FIX LỖI)
                # ---------------------------------------------------------
                sahi_count = 0
                if self.sahi_wrapper and SAHI_AVAILABLE:
                    try:
                        # 👉 ĐÃ FIX: Chuyển kích thước lát cắt thành 512x512 đồng bộ và kích hoạt thuật toán gộp box NMS chống trùng
                        sahi_res = get_sliced_prediction(
                            img_sahi, 
                            self.sahi_wrapper, 
                            slice_height=512, 
                            slice_width=512, 
                            overlap_height_ratio=0.2,
                            overlap_width_ratio=0.2,
                            postprocess_type="NMS",          # Bật NMS xóa trùng lặp box ở rìa mảnh cắt
                            postprocess_match_metric="IOU",
                            postprocess_match_threshold=0.4, # Ngưỡng gộp box trùng nhau > 40%
                            verbose=0
                        )
                        
                        for obj in sahi_res.object_prediction_list:
                            # 👉 ĐÃ FIX: Chỉ nhận diện 'person' (Class 0)
                            if obj.category.id == 0 or obj.category.name == 'person':
                                # 👉 ĐÃ FIX: Ép điều kiện lọc ngưỡng tự tin để loại bỏ cây cối, cột đèn nhận diện sai tỉ lệ thấp
                                if float(obj.score.value) < self.conf_level:
                                    continue
                                    
                                sahi_count += 1
                                b = obj.bbox

                                try:
                                    x1, y1, x2, y2 = b.to_xyxy()
                                except Exception:
                                    x1 = b.minx
                                    y1 = b.miny
                                    x2 = b.maxx
                                    y2 = b.maxy

                                cv2.rectangle(
                                    img_sahi,
                                    (int(x1), int(y1)),
                                    (int(x2), int(y2)),
                                    (0, 255, 255),
                                    2,
                                )

                                cv2.putText(
                                    img_sahi,
                                    f"P {float(obj.score.value):.2f}",
                                    (int(x1), int(y1) - 6),
                                    cv2.FONT_HERSHEY_SIMPLEX,
                                    0.4,
                                    (0, 255, 255),
                                    1,
                                )
                    except Exception as e:
                        print(f"[SAHI] Lỗi xử lý: {e}")
                        img_sahi = img_yolo.copy()
                        sahi_count = yolo_count
                else:
                    img_sahi = img_yolo.copy()
                    sahi_count = yolo_count
                    
                cv2.putText(img_sahi, f"Frame: {f_idx} | SAHI Total: {sahi_count}", (20, 40), 
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
                
                path_no_sahi = os.path.join(out_root, "no-sahi", v_stem)
                path_with_sahi = os.path.join(out_root, "with-sahi", v_stem)
                os.makedirs(path_no_sahi, exist_ok=True)
                os.makedirs(path_with_sahi, exist_ok=True)
                
                cv2.imwrite(os.path.join(path_no_sahi, f"frame_{f_idx}.jpg"), img_yolo)
                cv2.imwrite(os.path.join(path_with_sahi, f"frame_{f_idx}.jpg"), img_sahi)
                
        cap.release()
        print(f"[✓] Hoàn tất xử lý video: {v_stem}")

def main():
    root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    m_path = Config.YOLO_MODEL_PATH
    v_dir = os.path.join(root_path, "data", "video")
    out_dir = os.path.join(root_path, "output", "screenshots")
    
    os.makedirs(os.path.join(out_dir, "no-sahi"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "with-sahi"), exist_ok=True)
    
    print("=" * 60)
    print("Khởi tạo tiến trình trích xuất hình ảnh đối chứng YOLO vs SAHI")
    print("=" * 60)
    runner = ComparisonGenerator(m_path, Config.CONFIDENCE_THRESHOLD)
    
    exts = ('.mp4', '.avi', '.mov', '.mkv')
    if os.path.exists(v_dir):
        files = [os.path.join(v_dir, x) for x in os.listdir(v_dir) if x.lower().endswith(exts)]
        files.sort()
        if not files:
            print(f"[!] Không tìm thấy video trong: {v_dir}")
            return
        for video_item in files:
            print(f"\n[→] Đang phân tích mẫu ảnh cho video: {Path(video_item).name}")
            runner.process_and_save_samples(video_file=video_item, out_root=out_dir)
    else:
        print(f"[!] Thư mục video không tồn tại: {v_dir}")
        return
    
    print("\n" + "=" * 60)
    print("✓ Hoàn tất quy trình sinh ảnh mẫu so sánh trực quan!")
    print("=" * 60)

if __name__ == "__main__":
    main()