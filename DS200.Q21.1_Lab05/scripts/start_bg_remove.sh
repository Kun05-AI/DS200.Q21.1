#!/bin/bash
# Tiện ích xóa background

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
fi

echo ">>> Xử lý background..."
python3 "${ROOT_PATH}/src/background_remover.py" "$@"