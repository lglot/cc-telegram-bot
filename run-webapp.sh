#!/bin/bash
# Launcher cc-webapp (Mini App Telegram). Sources .env del bot (TG_TOKEN, ALLOW,
# CC_MODEL…) e avvia il server Starlette/uvicorn nel venv. Processo separato da bot.py.
set -a
source "$(dirname "$0")/.env"
set +a
PY="${PYTHON:-$(command -v python3)}"
[ -x "$PY" ] || { echo "python3 not found" >&2; exit 1; }
exec "$PY" "$(dirname "$0")/webapp/server.py"
