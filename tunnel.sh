#!/bin/bash
# Start Cloudflare quick tunnel for Mark bot
# Auto-restarts on failure, extracts public URL, updates Railway env var
# Usage: ./tunnel.sh

cd "$(dirname "$0")"

RAILWAY_TOKEN="${RAILWAY_TOKEN:-}"
LOCAL_PORT=8585

# Kill existing tunnel if running
if [ -f tunnel.pid ]; then
    OLD_PID=$(cat tunnel.pid)
    kill "$OLD_PID" 2>/dev/null
    rm -f tunnel.pid
fi

echo "Starting Cloudflare tunnel to localhost:$LOCAL_PORT..."

# Start tunnel and capture output
cloudflared tunnel --url http://localhost:$LOCAL_PORT 2>&1 | while read -r line; do
    echo "$line"
    # Extract the public URL when it appears
    if echo "$line" | grep -qo 'https://.*trycloudflare.com'; then
        URL=$(echo "$line" | grep -o 'https://.*trycloudflare.com')
        echo ""
        echo "============================================"
        echo "MARK PUBLIC URL: $URL"
        echo "============================================"
        echo ""
        echo "Set this in Railway for Alex:"
        echo "  MARK_URL=$URL"
        echo ""
        echo "$URL" > tunnel_url.txt
    fi
done
