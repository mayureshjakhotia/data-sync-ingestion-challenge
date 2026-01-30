#!/bin/bash
# Quick API discovery script - run FIRST when API key arrives
# Usage: API_KEY=your_key_here sh discover.sh

set -e

BASE="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API="$BASE/api/v1"

if [ -z "$API_KEY" ]; then
  echo "Usage: API_KEY=your_key sh discover.sh"
  exit 1
fi

echo "=========================================="
echo "  DataSync API Discovery"
echo "=========================================="

# 1. Dashboard HTML (look for hidden JS, API hints)
echo ""
echo "=== DASHBOARD HTML SOURCE ==="
curl -s "$BASE/?apiKey=$API_KEY" | head -100
echo ""

# 2. Standard events endpoint - check response structure & headers
echo ""
echo "=== EVENTS ENDPOINT (limit=2) - FULL HEADERS ==="
curl -s -D - -H "X-API-Key: $API_KEY" "$API/events?limit=2" 2>&1
echo ""

# 3. Probe undocumented endpoints
echo ""
echo "=== PROBING UNDOCUMENTED ENDPOINTS ==="
ENDPOINTS=(
  "/events/export"
  "/events/stream"
  "/events/download"
  "/events/bulk"
  "/events/ids"
  "/events/count"
  "/events/all"
  "/events/batch"
  "/export"
  "/stream"
  "/download"
  "/bulk"
  "/status"
  "/health"
  "/info"
  "/docs"
  "/metadata"
  "/schema"
  "/collections"
  "/channels"
  "/sources"
)

for ep in "${ENDPOINTS[@]}"; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" -H "X-API-Key: $API_KEY" "$API$ep")
  if [ "$STATUS" != "404" ] && [ "$STATUS" != "000" ]; then
    echo ">>> FOUND: $ep => HTTP $STATUS"
    echo "    Response preview:"
    curl -s -H "X-API-Key: $API_KEY" "$API$ep" | head -5
    echo ""
  else
    echo "    (skip) $ep => $STATUS"
  fi
done

# 4. Test page sizes
echo ""
echo "=== PAGE SIZE TESTS ==="
for SIZE in 100 500 1000 2000 5000 10000; do
  echo -n "limit=$SIZE => "
  RESP=$(curl -s -D /tmp/headers_$SIZE -H "X-API-Key: $API_KEY" "$API/events?limit=$SIZE")
  COUNT=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('data',[])))" 2>/dev/null || echo "parse_error")
  HASMORE=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('hasMore','?'))" 2>/dev/null || echo "?")
  KEYS=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(','.join(d.keys()))" 2>/dev/null || echo "?")
  echo "got $COUNT events | hasMore=$HASMORE | keys=$KEYS"
  echo "  Headers:"
  cat /tmp/headers_$SIZE | grep -i -E "rate|limit|x-|content-type|retry" || true
  echo ""
done

# 5. Test different Accept headers
echo ""
echo "=== ACCEPT HEADER TESTS ==="
for ACCEPT in "text/csv" "application/ndjson" "text/plain" "application/x-ndjson" "text/tab-separated-values"; do
  echo -n "Accept: $ACCEPT => "
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" -H "X-API-Key: $API_KEY" -H "Accept: $ACCEPT" "$API/events?limit=2")
  CT=$(curl -s -D - -o /dev/null -H "X-API-Key: $API_KEY" -H "Accept: $ACCEPT" "$API/events?limit=2" 2>&1 | grep -i content-type || true)
  echo "HTTP $STATUS | $CT"
  if [ "$STATUS" = "200" ]; then
    curl -s -H "X-API-Key: $API_KEY" -H "Accept: $ACCEPT" "$API/events?limit=2" | head -5
    echo ""
  fi
done

# 6. Test HTTP methods
echo ""
echo "=== HTTP METHOD TESTS ==="
for METHOD in OPTIONS HEAD POST; do
  echo -n "$METHOD /events => "
  curl -s -D - -o /dev/null -X "$METHOD" -H "X-API-Key: $API_KEY" "$API/events" 2>&1 | head -5
  echo ""
done

# 7. Sample event structure
echo ""
echo "=== SAMPLE EVENT STRUCTURE ==="
curl -s -H "X-API-Key: $API_KEY" "$API/events?limit=1" | python3 -m json.tool 2>/dev/null | head -40

echo ""
echo "=========================================="
echo "  Discovery complete!"
echo "=========================================="
