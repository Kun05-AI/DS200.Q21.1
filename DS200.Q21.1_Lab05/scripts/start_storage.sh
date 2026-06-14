#!/bin/bash
# Script khởi động Storage Server

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
fi

echo ">>> Khởi chạy Storage Server (JSON Persistence)..."
python3 "${ROOT_PATH}/src/result_storage_server.py" "$@"