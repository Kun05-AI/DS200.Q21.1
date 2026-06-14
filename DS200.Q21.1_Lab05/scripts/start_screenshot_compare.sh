#!/bin/bash
# Script so sánh kết quả SAHI vs YOLO

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
fi

echo ">>> Tạo ảnh so sánh (Comparison Generator)..."
python3 "${ROOT_PATH}/src/screenshot_comparator.py" "$@"
