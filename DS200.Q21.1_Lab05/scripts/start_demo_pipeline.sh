#!/bin/bash
# Demo runner script

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
fi

echo ">>> Demo chương trình..."
python3 "${ROOT_PATH}/src/end_to_end_demo.py" "$@"