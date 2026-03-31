#!/bin/bash
# Stop Mark bot
cd "$(dirname "$0")"
if [ -f mark.pid ]; then
    PID=$(cat mark.pid)
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "Mark stopped (PID $PID)"
    else
        echo "Mark was not running (stale PID)"
    fi
    rm -f mark.pid
else
    echo "No mark.pid file found"
fi
