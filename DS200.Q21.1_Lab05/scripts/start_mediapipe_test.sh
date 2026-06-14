#!/bin/bash
# Tiện ích MediaPipe

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
fi

echo ">>> Chạy MediaPipe Explorer..."
python3 "${ROOT_PATH}/src/mediapipe_test_lab.py" "$@"