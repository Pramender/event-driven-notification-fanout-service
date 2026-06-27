#!/usr/bin/env bash
# Manual API smoke test against a running fanout service.
# Usage:
#   export WEBHOOK_BASE="https://webhook.site/your-unique-id"   # or http://localhost:8089 for WireMock
#   ./src/test/resources/api-samples/manual-curls.sh

set -euo pipefail

API="${API_BASE:-http://localhost:8080}"
WEBHOOK="${WEBHOOK_BASE:?Set WEBHOOK_BASE to your webhook receiver URL (no trailing slash)}"
SAMPLES="$(cd "$(dirname "$0")" && pwd)"

patch_url() {
  sed "s|{{WEBHOOK_URL}}|${WEBHOOK}|g" "$1"
}

post_json() {
  local url="$1"
  local body="$2"
  local response
  local status
  response=$(curl -s -w "\n%{http_code}" -X POST "$url" -H 'Content-Type: application/json' -d "$body")
  status="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "$status" =~ ^2 ]]; then
    echo "$body"
  else
    echo "Request failed (HTTP $status): $body" >&2
    exit 1
  fi
}

assert_event_id() {
  local json="$1"
  local event_id
  event_id=$(echo "$json" | jq -r '.event_id // empty')
  if [ -z "$event_id" ] || [ "$event_id" = "null" ]; then
    echo "Expected event_id in response but got: $json" >&2
    exit 1
  fi
  echo "$event_id"
}

echo "==> Creating consumer 1: order-alerts"
CONSUMER1=$(curl -s -X POST "$API/v1/subscriptions" \
  -H 'Content-Type: application/json' \
  -d "$(patch_url "$SAMPLES/subscriptions/consumer-1-order-alerts.json")")
echo "$CONSUMER1" | jq .
ID1=$(echo "$CONSUMER1" | jq -r .subscriptionId)

echo "==> Creating consumer 2: payment-hooks"
CONSUMER2=$(curl -s -X POST "$API/v1/subscriptions" \
  -H 'Content-Type: application/json' \
  -d "$(patch_url "$SAMPLES/subscriptions/consumer-2-payment-hooks.json")")
echo "$CONSUMER2" | jq .
ID2=$(echo "$CONSUMER2" | jq -r .subscriptionId)

echo "==> Creating consumer 3: inventory-alerts"
CONSUMER3=$(curl -s -X POST "$API/v1/subscriptions" \
  -H 'Content-Type: application/json' \
  -d "$(patch_url "$SAMPLES/subscriptions/consumer-3-inventory-alerts.json")")
echo "$CONSUMER3" | jq .
ID3=$(echo "$CONSUMER3" | jq -r .subscriptionId)

echo "==> Listing subscriptions"
curl -s "$API/v1/subscriptions" | jq .

echo "==> Posting events"
ORDER=$(post_json "$API/v1/events" @"$SAMPLES/events/order-created-high-value.json")
echo "High-value order:" && echo "$ORDER" | jq .
EVENT1=$(assert_event_id "$ORDER")

post_json "$API/v1/events" @"$SAMPLES/events/order-created-low-value.json" | jq .

PAYMENT=$(post_json "$API/v1/events" @"$SAMPLES/events/payment-completed.json")
echo "Payment:" && echo "$PAYMENT" | jq .
EVENT2=$(assert_event_id "$PAYMENT")

INVENTORY=$(post_json "$API/v1/events" @"$SAMPLES/events/inventory-low-stock.json")
echo "Inventory:" && echo "$INVENTORY" | jq .
EVENT3=$(assert_event_id "$INVENTORY")

echo "==> Waiting 5s for delivery worker..."
sleep 5

echo "==> Audit by event (order)"
curl -s "$API/v1/audit/events/$EVENT1" | jq .

echo "==> Audit by subscription (payment consumer, SENT only)"
curl -s "$API/v1/audit/subscriptions/$ID2?status=SENT" | jq .

echo "==> Audit by subscription (inventory consumer, all statuses)"
curl -s "$API/v1/audit/subscriptions/$ID3" | jq .

DELIVERY_ID=$(curl -s "$API/v1/audit/events/$EVENT1" | jq -r '.[0].deliveryId')
echo "==> Audit by delivery $DELIVERY_ID"
curl -s "$API/v1/audit/deliveries/$DELIVERY_ID" | jq .

echo "==> Core smoke tests complete. Subscription IDs: $ID1, $ID2, $ID3"

echo "==> Replay historical events for consumer 1 (order-alerts, AT_LEAST_ONCE)"
# Replay runs last so ingest, delivery, and audit checks above are settled first.
# Sample events set occurred_at on 2026-06-27 (latest 10:15 UTC).
REPLAY_FROM="2026-06-27T00:00:00Z"
REPLAY_TO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
SAMPLE_LATEST_OCCURRED_AT="2026-06-27T10:15:00Z"
if [[ "$REPLAY_TO" < "$SAMPLE_LATEST_OCCURRED_AT" ]]; then
  REPLAY_TO="2026-06-27T23:59:59Z"
  echo "    (current UTC is before sample occurred_at; using end-of-day replay window)"
fi
echo "    replay window: $REPLAY_FROM → $REPLAY_TO"
REPLAY=$(curl -s -X POST "$API/v1/subscriptions/$ID1/replay" \
  -H 'Content-Type: application/json' \
  -d "{\"from\": \"$REPLAY_FROM\", \"to\": \"$REPLAY_TO\"}")
echo "$REPLAY" | jq '{eventsScanned, matched, queued, skipped}'

echo "==> Waiting 5s for replay delivery..."
sleep 5

echo "==> Audit by event after replay (order) — expect another attempt for AT_LEAST_ONCE"
curl -s "$API/v1/audit/events/$EVENT1" | jq .

echo "==> Done."
