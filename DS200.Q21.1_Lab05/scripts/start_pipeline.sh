#!/bin/bash
# Pipeline chạy toàn bộ hệ thống - [Optimized Version]

ROOT_PATH=$(readlink -f "$(dirname "$0")"/..)
VENV_PATH="${ROOT_PATH}/.venv/bin/activate"

if [ -f "${VENV_PATH}" ]; then
    source "${VENV_PATH}"
else
    echo "[WARN] Không tìm thấy virtualenv tại ${VENV_PATH}. Tiếp tục với Python hệ thống."
fi

PIDS=()

terminate_all() {
    echo -e "\n[!] Đang dừng các server..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
        fi
    done
    wait
    exit 0
}

trap terminate_all SIGINT SIGTERM

echo ">>> Khởi tạo pipeline tổng thể..."

python3 "${ROOT_PATH}/src/result_storage_server.py" "$@" & PIDS+=($!)
sleep 1

python3 "${ROOT_PATH}/src/detector_server.py" "$@" & PIDS+=($!)
sleep 1

python3 "${ROOT_PATH}/src/frame_receiver.py" "$@" & PIDS+=($!)
sleep 1

# Sender thường là client; nếu muốn khởi trong cùng máy, bật dòng dưới.
# Bạn có thể truyền --video / --frames khi gọi run_all.sh để sender dùng.
python3 "${ROOT_PATH}/src/frame_sender.py" "$@" & PIDS+=($!)
sleep 1

echo ">>> Hệ thống đã sẵn sàng. (Storage, Detector, Receiver, Sender)"
wait