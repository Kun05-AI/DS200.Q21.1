# -*- coding: utf-8 -*-
"""
Hệ thống Đếm Người trong Thời gian Thực - Môn học Phân tích Dữ liệu lớn DS200
Gói mã nguồn chính (Core Source Package) chứa các module thành phần:
- sender: Truyền tải khung hình qua giao thức TCP socket
- receiver: Tiếp nhận luồng dữ liệu và chuyển tiếp
- detect_object: Nhận diện đối tượng sử dụng mô hình YOLO/SAHI phối hợp PySpark
- storage_server: Lưu trữ kết quả phân tích dưới dạng cấu trúc JSON
- background_remover: Công cụ tách tách/xóa nền hình ảnh
- examine_mediapipe: Thử nghiệm giải pháp MediaPipe
"""

__version__ = "2.1.0-optimized"