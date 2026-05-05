#!/bin/bash
set -a
source "$(dirname "$0")/.env"
set +a
exec /opt/homebrew/bin/python3 "$(dirname "$0")/bot.py"
