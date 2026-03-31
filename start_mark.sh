#!/bin/bash
# Start Mark bot locally with nohup (survives Terminal closing)
# Usage: ./start_mark.sh
# Stop:  ./stop_mark.sh

cd "$(dirname "$0")"

# Kill existing instance if running
if [ -f mark.pid ]; then
    OLD_PID=$(cat mark.pid)
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping existing Mark (PID $OLD_PID)..."
        kill "$OLD_PID"
        sleep 2
    fi
    rm -f mark.pid
fi

# Start Mark in background
echo "Starting Mark bot locally..."
nohup python3 -m uvicorn mark_bot_final:app --host 0.0.0.0 --port 8585 > mark.log 2>&1 &
echo $! > mark.pid
echo "Mark started (PID $(cat mark.pid)) — listening on http://localhost:8585"
echo "Logs: tail -f ~/mark-bot/mark.log"
