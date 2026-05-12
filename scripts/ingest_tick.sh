#!/bin/bash

# Configuration
# Usage: ./ingest_tick.sh [SYMBOL] [PRICE] [HOST]
# Example: ./ingest_tick.sh AAPL 155.50 localhost:8080

SYMBOL=${1:-"AAPL"}
PRICE=${2:-"150.00"}
HOST=${3:-${MARKETMUX_HOST:-"localhost:8080"}}

echo "🚀 Sending tick: $SYMBOL @ $PRICE to $HOST..."

curl -X POST "http://$HOST/api/v1/ticks" \
     -H "Content-Type: application/json" \
     -d "{
           \"symbol\": \"$SYMBOL\",
           \"price\": $PRICE
         }"

echo -e "\n✅ Done."
