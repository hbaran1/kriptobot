#!/usr/bin/env bash
set -euo pipefail

export PANEL_HOST="${PANEL_HOST:-0.0.0.0}"
export PANEL_PORT="${PORT:-${PANEL_PORT:-5177}}"
export EXECUTOR_PORT="${EXECUTOR_PORT:-18080}"
export EXECUTOR_URL="${EXECUTOR_URL:-http://127.0.0.1:${EXECUTOR_PORT}}"
export PILOT_TOKEN="${PILOT_TOKEN:-change-me}"

python executor.py &
exec python panel.py
