#!/bin/bash

# Configuration
# Usage: ./ws_client.sh [SYMBOL] [HOST]
# Example: ./ws_client.sh GOOGL localhost:8080
# Note: Requires 'wscat' (npm install -g wscat)

SYMBOL=${1:-"AAPL"}
HOST=${2:-${MARKETMUX_HOST:-"localhost:8080"}}

if ! command -v wscat &> /dev/null
then
    echo "❌ Error: 'wscat' is not installed."
    echo "Install it using: npm install -g wscat"
    exit 1
fi

echo "🔌 Connecting to WebSocket at ws://$HOST/ws..."
echo "📡 Subscribing to $SYMBOL..."

# Run wscat and automatically send the subscription message
# We use a subshell to send the JSON after a short delay and then keep stdin open
{ sleep 1; echo "{\"action\":\"subscribe\", \"symbol\":\"$SYMBOL\"}"; cat; } | wscat -c "ws://$HOST/ws"
