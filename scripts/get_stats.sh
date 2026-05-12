#!/bin/bash

# Configuration
# Usage: ./get_stats.sh [HOST]
# Example: ./get_stats.sh 192.168.1.9:8080

HOST=${1:-${MARKETMUX_HOST:-"localhost:8080"}}

echo "📊 Fetching Gateway Stats from $HOST..."

curl -s "http://$HOST/stats" | jq . || curl -s "http://$HOST/stats"

echo -e "\n✅ Done."
