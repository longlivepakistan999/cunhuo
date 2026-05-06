#!/usr/bin/env bash
set -e
cd "$(dirname "$(readlink -f "$0")")"

# IMPORTANT: keep workers=1. The queue worker thread and live_jobs dict
# live in process memory; multiple workers would each run their own copy
# and double-execute / desync the queue.
if command -v gunicorn >/dev/null 2>&1; then
    exec gunicorn -w 1 -b 0.0.0.0:5000 --timeout 120 \
        --access-logfile - --error-logfile - app:app
else
    exec python3 app.py
fi
