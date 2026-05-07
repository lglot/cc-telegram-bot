#!/bin/bash
# Portable launcher. Sources .env, finds python3, exec.
# Override interpreter with PYTHON=/path/to/python.
set -a
source "$(dirname "$0")/.env"
set +a
PY="${PYTHON:-$(command -v python3)}"
[ -x "$PY" ] || { echo "python3 not found in PATH" >&2; exit 1; }
exec "$PY" "$(dirname "$0")/bot.py"
