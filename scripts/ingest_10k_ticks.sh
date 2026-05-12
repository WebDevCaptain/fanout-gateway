#!/bin/bash

# Configuration
HOST=${1:-${MARKETMUX_HOST:-"localhost:8080"}}
NUM_TICKS=10000
SYMBOLS=("AAPL" "GOOGL" "MSFT" "AMZN" "TSLA")

echo "🚀 Ingesting $NUM_TICKS ticks simultaneously into $HOST..."

# Function to send a single tick
send_tick() {
    local symbol=${SYMBOLS[$RANDOM % ${#SYMBOLS[@]}]}
    # Generate random price between 100 and 1000
    local price=$(awk "BEGIN {print 100 + rand()*900}")
    
    curl -s -X POST "http://$HOST/api/v1/ticks" \
         -H "Content-Type: application/json" \
         -d "{
               \"symbol\": \"$symbol\",
               \"price\": $price
             }" > /dev/null &
}

# Spawn 10,000 requests in the background
for i in $(seq 1 $NUM_TICKS); do
    send_tick
    
    # Optional: Small throttle every 500 requests to avoid local OS socket exhaustion
    if (( $i % 500 == 0 )); then
        echo "📡 Sent $i requests..."
    fi
done

echo "⏳ All requests spawned. Waiting for completion..."
wait
echo -e "\n✅ Done. 10k ticks sent."
