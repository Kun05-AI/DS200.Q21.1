import argparse
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MediaPipeFeatureExplorer")

try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    logger.error("Thiếu thư viện OpenCV.")

try:
    import mediapipe as mp
    MP_AVAILABLE = True
except ImportError:
    MP_AVAILABLE = False
    logger.error("Thiếu thư viện MediaPipe.")

class MediaPipeExplorer:
    """Module khảo sát khả năng phân tích điểm mốc cơ thể (Face, Hands, Pose) của framework MediaPipe."""
    
    def __init__(self):
        self.mp_draw_utils = None
        self.mp_draw_styles = None
        self.solution_pose = None
        self.solution_face = None
        self.solution_hands = None
        
        if MP_AVAILABLE:
            self._init_solutions()
            
    def _init_solutions(self):
        """Nạp các giải pháp đồ họa và mô hình học máy của MediaPipe."""
        self.mp_draw_utils = mp.solutions.drawing_utils
        self.mp_draw_styles = mp.solutions.drawing_styles
        self.solution_pose = mp.solutions.pose
        self.solution_face = mp.solutions.face_detection
        self.solution_hands = mp.solutions.hands

    def process_file(self, img_path: str, out_root: str, mode: str = "all"):
        """Đọc file ảnh, chạy phân tích vẽ các khung xương, điểm mốc và xuất file báo cáo."""
        if not CV2_AVAILABLE or not MP_AVAILABLE:
            return
            
        source_image = cv2.imread(img_path)
        if source_image is None:
            logger.error(f"Không thể mở file ảnh chỉ định: {img_path}")
            return
            
        os.makedirs(out_root, exist_ok=True)
        filename = os.path.basename(img_path)
        rgb_img = cv2.cvtColor(source_image, cv2.COLOR_BGR2RGB)
        
        # Phân tích Pose
        if mode in ("pose", "all"):
            annotated = source_image.copy()
            with self.solution_pose.Pose(static_image_mode=True, min_detection_confidence=0.5) as pose_engine:
                res = pose_engine.process(rgb_img)
                if res.pose_landmarks:
                    self.mp_draw_utils.draw_landmarks(
                        annotated, res.pose_landmarks, self.solution_pose.POSE_CONNECTIONS,
                        landmark_drawing_spec=self.mp_draw_styles.get_default_pose_landmarks_style()
                    )
            cv2.imwrite(os.path.join(out_root, f"pose_{filename}"), annotated)
            
        # Phân tích Hands
        if mode in ("hands", "all"):
            annotated = source_image.copy()
            with self.solution_hands.Hands(static_image_mode=True, max_num_hands=2, min_detection_confidence=0.5) as hands_engine:
                res = hands_engine.process(rgb_img)
                if res.multi_hand_landmarks:
                    for hand_pts in res.multi_hand_landmarks:
                        self.mp_draw_utils.draw_landmarks(
                            annotated, hand_pts, self.solution_hands.HAND_CONNECTIONS,
                            self.mp_draw_styles.get_default_hand_landmarks_style(),
                            self.mp_draw_styles.get_default_hand_connections_style()
                        )
            cv2.imwrite(os.path.join(out_root, f"hands_{filename}"), annotated)
            
        # Phân tích Face
        if mode in ("face", "all"):
            annotated = source_image.copy()
            with self.solution_face.FaceDetection(min_detection_confidence=0.5) as face_engine:
                res = face_engine.process(rgb_img)
                if res.detections:
                    for det in res.detections:
                        self.mp_draw_utils.draw_detection(annotated, det)
            cv2.imwrite(os.path.join(out_root, f"face_{filename}"), annotated)
            
        logger.info(f"Đã xử lý xong các tác vụ MediaPipe Explorer cho tệp: {filename}")

def demo():
    logger.info("Chạy chương trình demo MediaPipe Explorer...")

def main():
    parser = argparse.ArgumentParser(description="Công cụ nghiên cứu trực quan hóa các giải pháp MediaPipe Hub.")
    parser.add_argument("--input", "-i", help="Đường dẫn file hình ảnh trích xuất dữ liệu")
    parser.add_argument("--output", "-o", default="output/mediapipe", help="Thư mục kết quả kết xuất")
    parser.add_argument("--mode", "-m", default="all", choices=["pose", "face", "hands", "all"])
    parser.add_argument("--demo", "-d", action="store_true")
    
    args = parser.parse_args()
    if args.demo:
        demo()
        return
        
    if not args.input:
        parser.print_help()
        return
        
    explorer = MediaPipeExplorer()
    explorer.process_file(args.input, args.output, mode=args.mode)

if __name__ == "__main__":
    main()