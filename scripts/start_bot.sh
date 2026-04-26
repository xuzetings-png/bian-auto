#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -d ".venv" ]]; then
  echo "未找到 .venv，请先执行: python3 -m venv .venv && source .venv/bin/activate && pip install -e ."
  exit 1
fi

source ".venv/bin/activate"
exec bot
