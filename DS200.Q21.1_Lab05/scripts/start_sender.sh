#!/bin/bash
# Script khởi động Frame Sender

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
fi

echo ">>> Bắt đầu gửi dữ liệu frame..."
# Ví dụ: ./run_sender.sh --video data/video/walk.mp4 --frames 200 --fps 5
python3 "${ROOT_PATH}/src/frame_sender.py" "$@"