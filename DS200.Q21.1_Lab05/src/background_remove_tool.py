import argparse
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("ImageBackgroundRemover")

try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    logger.error("Không tìm thấy mô-đun OpenCV cv2.")

try:
    import mediapipe as mp
    MP_AVAILABLE = True
except ImportError:
    MP_AVAILABLE = False
    logger.warning("Không tìm thấy mô-đun MediaPipe.")

class BackgroundRemover:
    """Tiện ích tách/xóa và thay đổi nền của ảnh chân dung sử dụng MediaPipe Selfie Segmentation."""
    
    def __init__(self, model_selection=1):
        self.model_mode = model_selection
        self.pipe_engine = None
        
        if MP_AVAILABLE:
            self._setup_mediapipe_engine()
        else:
            logger.warning("Sử dụng kịch bản dự phòng truyền thống dựa trên OpenCV.")
            
    def _setup_mediapipe_engine(self):
        """Khởi động giải pháp phân mảnh thực thể của MediaPipe."""
        try:
            mp_segmentation = mp.solutions.selfie_segmentation
            self.pipe_engine = mp_segmentation.SelfieSegmentation(model_selection=self.model_mode)
            logger.info("Đã cấu hình bộ lọc phông nền MediaPipe thành công.")
        except Exception as e:
            logger.error(f"Thất bại khi liên kết MediaPipe: {e}")
            self.pipe_engine = None

    def process_file(self, file_path: str, out_path: str = None, bg_color=(0, 255, 0), apply_blur=False):
        """Thực hiện bóc tách thực thể người ra khỏi nền hình ảnh."""
        if not CV2_AVAILABLE:
            logger.error("OpenCV không khả dụng. Không thể xử lý ảnh.")
            return
        img = cv2.imread(file_path)
        if img is None:
            logger.error(f"Tệp hình ảnh không khả dụng hoặc hỏng: {file_path}")
            return
            
        if self.pipe_engine:
            rgb_format = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            inference_output = self.pipe_engine.process(rgb_format)
            seg_mask = getattr(inference_output, "segmentation_mask", None)
            if seg_mask is None:
                logger.warning("MediaPipe không trả về segmentation_mask, dùng fallback.")
                final_rendering = cv2.blur(img, (15, 15)) if apply_blur else img
            else:
                binary_mask = seg_mask > 0.5
                condition = np.stack((binary_mask,) * 3, axis=-1)
                
                if apply_blur:
                    background_layer = cv2.GaussianBlur(img, (55, 55), 0)
                elif bg_color is None:
                    h, w, _ = img.shape
                    transparent_layer = np.zeros((h, w, 4), dtype=np.uint8)
                    img_rgba = cv2.cvtColor(img, cv2.COLOR_BGR2BGRA)
                    condition_4ch = np.stack((binary_mask,) * 4, axis=-1)
                    final_rendering = np.where(condition_4ch, img_rgba, transparent_layer)
                    
                    if out_path is None:
                        base, _ = os.path.splitext(file_path)
                        out_path = f"{base}_nobg.png"
                    cv2.imwrite(out_path, final_rendering)
                    logger.info(f"Đã xuất tệp ảnh trong suốt: {out_path}")
                    return
                else:
                    background_layer = np.zeros(img.shape, dtype=np.uint8)
                    background_layer[:] = bg_color
                    
                final_rendering = np.where(condition, img, background_layer)
        else:
            final_rendering = cv2.blur(img, (15, 15)) if apply_blur else img
            
        if out_path is None:
            base, ext = os.path.splitext(file_path)
            out_path = f"{base}_processed{ext}"
        cv2.imwrite(out_path, final_rendering)
        logger.info(f"Đã xuất tệp ảnh thành công tại: {out_path}")

    def close(self):
        """Đóng an toàn phiên làm việc của bộ gom tài nguyên."""
        try:
            if self.pipe_engine:
                try:
                    self.pipe_engine.close()
                except Exception:
                    try:
                        del self.pipe_engine
                    except Exception:
                        pass
                self.pipe_engine = None
        except Exception:
            pass

def demo():
    logger.info("Chạy kịch bản demo mẫu của module BackgroundRemover...")
    sample_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "images")
    sample_dir = os.path.abspath(sample_dir)
    if os.path.exists(sample_dir):
        for f in os.listdir(sample_dir):
            if f.lower().endswith((".jpg", ".jpeg", ".png")):
                path = os.path.join(sample_dir, f)
                remover = BackgroundRemover()
                remover.process_file(path, None, bg_color=(0,255,0), apply_blur=False)
                remover.close()
                break

def main():
    parser = argparse.ArgumentParser(description="Ứng dụng xóa phông ảnh chuyên biệt sử dụng mạng nơ-ron.")
    parser.add_argument("--input", "-i", help="Đường dẫn file ảnh đầu vào")
    parser.add_argument("--output", "-o", help="Đường dẫn kết quả đầu ra")
    parser.add_argument("--color", "-c", default="green", choices=["green", "blue", "white", "black", "transparent"])
    parser.add_argument("--blur", "-b", action="store_true", help="Làm mờ ảnh nền gốc thay vì chèn màu đơn sắc")
    parser.add_argument("--demo", "-d", action="store_true", help="Kích hoạt kịch bản thử nghiệm nhanh")
    
    args = parser.parse_args()
    if args.demo:
        demo()
        return
        
    if not args.input:
        parser.print_help()
        return
    
    # ✅ FIX: Bỏ dòng 145-146 dư thừa - chỉ giữ lại dòng 144
    cmap = {"green": (0, 255, 0), "blue": (255, 0, 0), "white": (255, 255, 255), "black": (0, 0, 0), "transparent": None}
    bg_choice = None if args.color == "transparent" else cmap.get(args.color, (0, 255, 0))
    
    processor = BackgroundRemover()
    processor.process_file(args.input, args.output, bg_color=bg_choice, apply_blur=args.blur)
    processor.close()

if __name__ == "__main__":
    main()