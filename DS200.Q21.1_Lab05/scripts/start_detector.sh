#!/bin/bash
# Script khởi động Detector Server

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
fi

echo ">>> Khởi chạy Object Detection Server..."
python3 "${ROOT_PATH}/src/detector_server.py" "$@"