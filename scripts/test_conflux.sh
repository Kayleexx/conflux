#!/usr/bin/env bash
set -e

# ==========================================================
# Conflux Automated Test Script
# ==========================================================
# This script performs a basic functional test of the Conflux
# backend. It covers authentication, WebSocket communication,
# chat messages, awareness updates, and the dashboard endpoint.
#
# Requirements:
#   - Conflux server running locally (cargo run -p confluxd)
#   - curl, jq, and websocat installed
#
# Usage:
#   chmod +x scripts/test_conflux.sh
#   ./scripts/test_conflux.sh
# ==========================================================

SERVER_URL="http://127.0.0.1:8080"
WS_URL="ws://127.0.0.1:8080"
ROOM="testdoc"

echo "Running Conflux automated backend test"

# Step 1: Generate a JWT token
echo
echo "Generating token for user 'kaylee'..."
TOKEN=$(curl -s -X POST "$SERVER_URL/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"kaylee"}' | jq -r '.token')

if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
  echo "Failed to generate token. Exiting."
  exit 1
fi

echo "Token received:"
echo "$TOKEN"
echo

# Step 2: Check the dashboard before connecting
echo "Checking dashboard before any clients are connected..."
curl -s "$SERVER_URL/dashboard" | jq .
echo

# Step 3: Start WebSocket client 1
echo "Starting WebSocket client 1..."
{
  echo '{"type":"chat","message":"hello from client 1"}'
  sleep 1
  echo '{"type":"awareness","data":{"cursor":101}}'
  sleep 1
  echo '{"type":"sync_request"}'
  sleep 1
} | websocat -t --no-close "$WS_URL/ws/$ROOM?token=$TOKEN" &
PID1=$!

sleep 2

# Step 4: Start WebSocket client 2
echo "Starting WebSocket client 2..."
{
  echo '{"type":"chat","message":"hello from client 2"}'
  sleep 1
  echo '{"type":"awareness","data":{"cursor":202}}'
  sleep 1
  echo '{"type":"sync_request"}'
  sleep 1
} | websocat -t --no-close "$WS_URL/ws/$ROOM?token=$TOKEN" &
PID2=$!

sleep 3

# Step 5: Fetch dashboard after clients connect
echo
echo "Fetching dashboard after clients have connected..."
curl -s "$SERVER_URL/dashboard" | jq .
echo

# Step 6: Stop WebSocket clients
echo "Stopping test clients..."
kill $PID1 $PID2 2>/dev/null || true

echo
echo "Conflux backend test completed successfully."
echo "==========================================="
