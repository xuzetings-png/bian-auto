#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p logs

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] 启动 bot 进程" | tee -a logs/guard.log
  ./scripts/start_bot.sh >> logs/guard.log 2>&1 || true
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] bot 退出，5 秒后重启" | tee -a logs/guard.log
  sleep 5
done
