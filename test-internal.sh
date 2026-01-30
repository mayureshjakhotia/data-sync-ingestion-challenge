#!/usr/bin/env bash
# test-internal.sh - Probe all internal/hidden endpoints on the DataSync API
# This is part of the coding challenge where API discovery is 60% of the score.

set -euo pipefail

BASE="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API_KEY="test"
RESULTS_DIR="/tmp/internal-probe-results"
SUMMARY_FILE="${RESULTS_DIR}/summary.txt"

mkdir -p "$RESULTS_DIR"
> "$SUMMARY_FILE"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

probe_endpoint() {
  local url="$1"
  local label="${2:-$url}"
  local extra_headers="${3:-}"

  # Build curl command
  local curl_cmd="curl -s -o /tmp/probe_body.txt -w '%{http_code}|%{size_download}|%{content_type}' -D /tmp/probe_headers.txt --max-time 15"

  if [ -n "$extra_headers" ]; then
    curl_cmd="$curl_cmd $extra_headers"
  fi

  curl_cmd="$curl_cmd '$url'"

  local result
  result=$(eval $curl_cmd 2>/dev/null) || true

  local status=$(echo "$result" | cut -d'|' -f1)
  local size=$(echo "$result" | cut -d'|' -f2)
  local content_type=$(echo "$result" | cut -d'|' -f3)

  # Skip 404s in verbose output but still log
  if [ "$status" = "404" ]; then
    echo -e "  ${RED}[404]${NC} $label"
    return
  fi

  # Non-404: print details
  echo ""
  echo -e "  ${GREEN}[$status]${NC} ${CYAN}$label${NC}"
  echo -e "    Content-Type: $content_type"
  echo -e "    Size: $size bytes"

  # Print interesting headers
  if [ -f /tmp/probe_headers.txt ]; then
    local interesting_headers
    interesting_headers=$(grep -iE '^(x-|ratelimit|cache|etag|vary|retry|link|access-control|server|content-encoding)' /tmp/probe_headers.txt 2>/dev/null || true)
    if [ -n "$interesting_headers" ]; then
      echo -e "    ${YELLOW}Headers:${NC}"
      echo "$interesting_headers" | while read -r hdr; do
        echo "      $hdr"
      done
    fi
  fi

  # Print first 500 chars of body
  if [ -f /tmp/probe_body.txt ] && [ -s /tmp/probe_body.txt ]; then
    local body_preview
    body_preview=$(head -c 500 /tmp/probe_body.txt)
    echo -e "    ${YELLOW}Body preview:${NC}"
    echo "$body_preview" | head -20 | sed 's/^/      /'
  fi

  # Log to summary
  echo "[$status] $size bytes | $content_type | $label" >> "$SUMMARY_FILE"

  echo ""
}

echo "============================================================"
echo " DataSync Internal Endpoint Discovery"
echo " Base: $BASE"
echo " API Key: $API_KEY"
echo "============================================================"
echo ""

# ========================================================================
# SECTION 1: /internal/ endpoints with ?apiKey=test
# ========================================================================
echo "================================================================"
echo "SECTION 1: /internal/ endpoints (with ?apiKey=test)"
echo "================================================================"

INTERNAL_PATHS=(
  "/internal/stats"
  "/internal/events"
  "/internal/events/count"
  "/internal/events/all"
  "/internal/events/export"
  "/internal/events/stream"
  "/internal/events/download"
  "/internal/events/bulk"
  "/internal/events/ids"
  "/internal/events/dump"
  "/internal/events/raw"
  "/internal/events/data"
  "/internal/users"
  "/internal/sessions"
  "/internal/metrics"
  "/internal/config"
  "/internal/cache"
  "/internal/redis"
  "/internal/db"
  "/internal/health"
  "/internal/debug"
  "/internal/status"
  "/internal/info"
  "/internal/schema"
  "/internal/api"
  "/internal/routes"
  "/internal/endpoints"
  "/internal/docs"
  "/internal/admin"
  "/internal/export"
  "/internal/download"
  "/internal/dump"
  "/internal/backup"
  "/internal/snapshot"
  "/internal/firehose"
  "/internal/stream"
  "/internal/bulk"
  "/internal/batch"
  "/internal/query"
  "/internal/search"
)

for path in "${INTERNAL_PATHS[@]}"; do
  probe_endpoint "${BASE}${path}?apiKey=${API_KEY}" "$path"
done

# ========================================================================
# SECTION 2: Root-level hidden paths
# ========================================================================
echo ""
echo "================================================================"
echo "SECTION 2: Root-level / alternate paths (with ?apiKey=test)"
echo "================================================================"

ROOT_PATHS=(
  "/debug/events"
  "/debug/stats"
  "/admin/events"
  "/admin/stats"
  "/admin/export"
  "/metrics"
  "/prometheus"
  "/_internal/events"
  "/api/internal/events"
  "/api/v1/internal/events"
  "/api/v1/internal/stats"
)

for path in "${ROOT_PATHS[@]}"; do
  probe_endpoint "${BASE}${path}?apiKey=${API_KEY}" "$path"
done

# ========================================================================
# SECTION 3: For successful /internal/ endpoints, try extra params
# ========================================================================
echo ""
echo "================================================================"
echo "SECTION 3: Extended probes on successful /internal/ endpoints"
echo "================================================================"

# Collect the successful internal endpoints from summary
SUCCESSFUL_INTERNAL=()
while IFS= read -r line; do
  status=$(echo "$line" | grep -oP '^\[\K[0-9]+' || true)
  path=$(echo "$line" | grep -oP '\| /internal/\S+' | sed 's/^| //' || true)
  if [ "$status" = "200" ] && [ -n "$path" ]; then
    SUCCESSFUL_INTERNAL+=("$path")
  fi
done < "$SUMMARY_FILE"

echo "Found ${#SUCCESSFUL_INTERNAL[@]} successful /internal/ endpoints to probe further."

EXTRA_PARAMS=(
  "format=csv"
  "format=ndjson"
  "format=json"
  "limit=10"
  "type=page_view"
  "type=click"
  "all=true"
  "export=true"
  "ids=true"
  "count=true"
)

for endpoint in "${SUCCESSFUL_INTERNAL[@]}"; do
  echo ""
  echo "--- Extended probes for $endpoint ---"

  for param in "${EXTRA_PARAMS[@]}"; do
    probe_endpoint "${BASE}${endpoint}?apiKey=${API_KEY}&${param}" "${endpoint}?${param}"
  done
done

# ========================================================================
# SECTION 4: /api/v1/events filtering with both auth methods
# ========================================================================
echo ""
echo "================================================================"
echo "SECTION 4: /api/v1/events filtering (both auth methods)"
echo "================================================================"

FILTER_PARAMS=(
  "limit=5&type=page_view"
  "limit=5&type=click"
  "limit=5&eventType=page_view"
  "limit=5&event_type=page_view"
  "limit=5&filter=type:page_view"
  "limit=5&device=mobile"
  "limit=5&deviceType=mobile"
  "limit=5&userId=1"
  "limit=5&sessionId=1"
  "limit=5&source=web"
)

echo ""
echo "--- Using apiKey=test as query param ---"
for params in "${FILTER_PARAMS[@]}"; do
  probe_endpoint "${BASE}/api/v1/events?apiKey=${API_KEY}&${params}" "/api/v1/events?${params} (query key)"
done

echo ""
echo "--- Using X-API-Key: test header ---"
for params in "${FILTER_PARAMS[@]}"; do
  probe_endpoint "${BASE}/api/v1/events?${params}" "/api/v1/events?${params} (header key)" "-H 'X-API-Key: ${API_KEY}'"
done

# ========================================================================
# SECTION 5: Cache bypass tests on /internal/stats
# ========================================================================
echo ""
echo "================================================================"
echo "SECTION 5: Cache bypass tests on /internal/stats"
echo "================================================================"

echo ""
echo "--- Cache-Control: no-cache ---"
probe_endpoint "${BASE}/internal/stats?apiKey=${API_KEY}" "/internal/stats + Cache-Control: no-cache" "-H 'Cache-Control: no-cache'"

echo "--- X-Cache-Bypass: true ---"
probe_endpoint "${BASE}/internal/stats?apiKey=${API_KEY}" "/internal/stats + X-Cache-Bypass: true" "-H 'X-Cache-Bypass: true'"

echo "--- X-No-Cache: true ---"
probe_endpoint "${BASE}/internal/stats?apiKey=${API_KEY}" "/internal/stats + X-No-Cache: true" "-H 'X-No-Cache: true'"

echo "--- ?nocache=1 ---"
probe_endpoint "${BASE}/internal/stats?apiKey=${API_KEY}&nocache=1" "/internal/stats?nocache=1"

echo "--- ?_cache=false ---"
probe_endpoint "${BASE}/internal/stats?apiKey=${API_KEY}&_cache=false" "/internal/stats?_cache=false"

echo "--- ?refresh=true ---"
probe_endpoint "${BASE}/internal/stats?apiKey=${API_KEY}&refresh=true" "/internal/stats?refresh=true"

# Also test cache bypass on /internal/events if it was successful
if printf '%s\n' "${SUCCESSFUL_INTERNAL[@]}" | grep -q '/internal/events'; then
  echo ""
  echo "--- Cache bypass on /internal/events ---"
  probe_endpoint "${BASE}/internal/events?apiKey=${API_KEY}&nocache=1" "/internal/events?nocache=1"
  probe_endpoint "${BASE}/internal/events?apiKey=${API_KEY}" "/internal/events + Cache-Control: no-cache" "-H 'Cache-Control: no-cache'"
fi

# ========================================================================
# FINAL SUMMARY
# ========================================================================
echo ""
echo "============================================================"
echo " SUMMARY: All non-404 responses"
echo "============================================================"
echo ""
if [ -f "$SUMMARY_FILE" ]; then
  cat "$SUMMARY_FILE"
else
  echo "(no results)"
fi

echo ""
echo "Results also saved to: $SUMMARY_FILE"
echo "Done."
