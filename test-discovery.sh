#!/bin/bash
###############################################################################
# test-discovery.sh - Comprehensive endpoint and asset discovery script
#
# Usage: API_KEY=your_key_here sh test-discovery.sh
#
# Discovers:
#   - Dashboard HTML source and embedded JS bundles
#   - API routes, secrets, endpoints hidden in JS code
#   - Alternative protocol endpoints (WebSocket, SSE, GraphQL, gRPC)
#   - Deep endpoint enumeration (GET + POST)
#   - robots.txt, sitemap.xml, .well-known, OpenAPI/Swagger specs
#   - Common framework health/debug/metrics paths
###############################################################################

set -euo pipefail

BASE="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API="$BASE/api/v1"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

log_section() { echo ""; echo "${BOLD}${CYAN}=== $1 ===${NC}"; }
log_found()   { echo "  ${GREEN}[FOUND]${NC} $1"; }
log_miss()    { echo "  ${RED}[MISS]${NC}  $1"; }
log_info()    { echo "  ${YELLOW}[INFO]${NC}  $1"; }

if [ -z "${API_KEY:-}" ]; then
  echo "Usage: API_KEY=your_key sh test-discovery.sh"
  exit 1
fi

AUTH_HEADER="X-API-Key: $API_KEY"

###############################################################################
# Helper: probe a URL, report status, save body if non-404
###############################################################################
probe() {
  local url="$1"
  local method="${2:-GET}"
  local label="${3:-$url}"
  local extra_args="${4:-}"

  local status body
  if [ "$method" = "POST" ]; then
    status=$(curl -s -o "$TMPDIR/body.tmp" -w "%{http_code}" -X POST \
      -H "$AUTH_HEADER" -H "Content-Type: application/json" \
      $extra_args "$url" 2>/dev/null || echo "000")
  else
    status=$(curl -s -o "$TMPDIR/body.tmp" -w "%{http_code}" \
      -H "$AUTH_HEADER" $extra_args "$url" 2>/dev/null || echo "000")
  fi

  if [ "$status" != "404" ] && [ "$status" != "000" ]; then
    local preview
    preview=$(head -c 200 "$TMPDIR/body.tmp" 2>/dev/null | tr '\n' ' ' | head -c 200)
    log_found "$method $label => HTTP $status  |  ${preview:0:150}"
    return 0
  else
    log_miss "$method $label => HTTP $status"
    return 1
  fi
}

echo ""
echo "${BOLD}######################################################${NC}"
echo "${BOLD}  DataSync Comprehensive Discovery Script${NC}"
echo "${BOLD}######################################################${NC}"
echo "  Base: $BASE"
echo "  API:  $API"
echo "  Time: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

###############################################################################
# 1. DASHBOARD HTML SOURCE ANALYSIS
###############################################################################
log_section "1. DASHBOARD HTML SOURCE ANALYSIS"

# Fetch dashboard HTML
log_info "Fetching dashboard HTML..."
DASH_HTML="$TMPDIR/dashboard.html"
curl -s -H "$AUTH_HEADER" "$BASE/?apiKey=$API_KEY" > "$DASH_HTML" 2>/dev/null
curl -s -H "$AUTH_HEADER" "$BASE/" > "$TMPDIR/dashboard_nokey.html" 2>/dev/null

DASH_SIZE=$(wc -c < "$DASH_HTML" | tr -d ' ')
log_info "Dashboard HTML size: $DASH_SIZE bytes"

# Show first 50 lines
echo ""
log_info "Dashboard HTML head (first 50 lines):"
head -50 "$DASH_HTML" | sed 's/^/    /'
echo ""

# Extract JS bundle URLs
log_info "Extracting JavaScript bundle URLs..."
JS_URLS=$(grep -oE '(src|href)="[^"]*\.js[^"]*"' "$DASH_HTML" 2>/dev/null | \
  sed -E 's/(src|href)="([^"]*)"/\2/' | sort -u || true)

# Also look for inline script src patterns
JS_URLS2=$(grep -oE 'src="[^"]*"' "$DASH_HTML" 2>/dev/null | \
  sed 's/src="//;s/"//' | grep -E '\.(js|mjs|jsx|ts|tsx|bundle|chunk)' | sort -u || true)

ALL_JS=$(echo -e "$JS_URLS\n$JS_URLS2" | sort -u | grep -v '^$' || true)

if [ -n "$ALL_JS" ]; then
  log_found "JavaScript bundles found:"
  echo "$ALL_JS" | while read -r jsurl; do
    # Resolve relative URLs
    if echo "$jsurl" | grep -qE '^https?://'; then
      FULL_URL="$jsurl"
    elif echo "$jsurl" | grep -q '^/'; then
      FULL_URL="$BASE$jsurl"
    else
      FULL_URL="$BASE/$jsurl"
    fi
    echo "    $FULL_URL"
  done
else
  log_info "No external JS bundles found - checking for inline scripts"
fi

# Extract CSS URLs
CSS_URLS=$(grep -oE 'href="[^"]*\.css[^"]*"' "$DASH_HTML" 2>/dev/null | \
  sed 's/href="//;s/"//' | sort -u || true)
if [ -n "$CSS_URLS" ]; then
  log_info "CSS assets found:"
  echo "$CSS_URLS" | sed 's/^/    /'
fi

# Extract all href/src attributes
ALL_ASSETS=$(grep -oE '(src|href|action)="[^"]*"' "$DASH_HTML" 2>/dev/null | \
  sed -E 's/(src|href|action)="([^"]*)"/\2/' | sort -u || true)
if [ -n "$ALL_ASSETS" ]; then
  log_info "All asset URLs in HTML:"
  echo "$ALL_ASSETS" | sed 's/^/    /'
fi

###############################################################################
# 1b. FETCH AND ANALYZE JS BUNDLES
###############################################################################
log_section "1b. JS BUNDLE DEEP ANALYSIS"

if [ -n "$ALL_JS" ]; then
  BUNDLE_IDX=0
  echo "$ALL_JS" | while read -r jsurl; do
    BUNDLE_IDX=$((BUNDLE_IDX + 1))
    if echo "$jsurl" | grep -qE '^https?://'; then
      FULL_URL="$jsurl"
    elif echo "$jsurl" | grep -q '^/'; then
      FULL_URL="$BASE$jsurl"
    else
      FULL_URL="$BASE/$jsurl"
    fi

    BUNDLE_FILE="$TMPDIR/bundle_${BUNDLE_IDX}.js"
    log_info "Fetching JS bundle: $FULL_URL"
    curl -s "$FULL_URL" > "$BUNDLE_FILE" 2>/dev/null
    BSIZE=$(wc -c < "$BUNDLE_FILE" | tr -d ' ')
    log_info "  Bundle size: $BSIZE bytes"

    # Search for API routes
    echo ""
    log_info "  API routes in bundle (patterns: /api/, fetch, axios):"
    grep -oE '/api/[a-zA-Z0-9/_-]+' "$BUNDLE_FILE" 2>/dev/null | sort -u | head -50 | sed 's/^/      /' || echo "      (none)"

    log_info "  Fetch/axios calls:"
    grep -oE 'fetch\([^)]{0,200}\)' "$BUNDLE_FILE" 2>/dev/null | head -20 | sed 's/^/      /' || echo "      (none)"
    grep -oE 'axios\.[a-z]+\([^)]{0,200}\)' "$BUNDLE_FILE" 2>/dev/null | head -20 | sed 's/^/      /' || echo "      (none)"

    # Search for endpoint strings
    log_info "  Endpoint-like strings:"
    grep -oE '"[/][a-zA-Z0-9/_.-]+"' "$BUNDLE_FILE" 2>/dev/null | sort -u | head -30 | sed 's/^/      /' || echo "      (none)"

    # Search for WebSocket URLs
    log_info "  WebSocket URLs (ws://, wss://):"
    grep -oE 'wss?://[^"'"'"' ]+' "$BUNDLE_FILE" 2>/dev/null | sort -u | head -10 | sed 's/^/      /' || echo "      (none)"

    # Search for GraphQL
    log_info "  GraphQL references:"
    grep -iE '(graphql|gql|query\s*\{|mutation\s*\{)' "$BUNDLE_FILE" 2>/dev/null | head -10 | sed 's/^/      /' || echo "      (none)"

    # Search for environment variables / config
    log_info "  Env vars / config keys:"
    grep -oE '(REACT_APP_|NEXT_PUBLIC_|VITE_|process\.env\.[A-Z_]+|API_URL|BASE_URL|API_KEY|SECRET|TOKEN|ENDPOINT)[A-Z_a-z0-9]*' "$BUNDLE_FILE" 2>/dev/null | sort -u | head -20 | sed 's/^/      /' || echo "      (none)"

    # Search for interesting strings
    log_info "  Interesting strings (export|stream|bulk|download|dump|firehose|batch|ndjson|csv|websocket|sse):"
    grep -oiE '(export|stream|bulk|download|dump|firehose|batch|ndjson|csv|websocket|sse|server.sent|event.source)[a-zA-Z0-9_/-]*' "$BUNDLE_FILE" 2>/dev/null | sort -u | head -20 | sed 's/^/      /' || echo "      (none)"

    echo ""
  done
else
  log_info "No JS bundles to analyze"
fi

# Also search inline scripts in the HTML
log_info "Searching inline scripts in dashboard HTML..."
grep -oE '/api/[a-zA-Z0-9/_-]+' "$DASH_HTML" 2>/dev/null | sort -u | head -20 | sed 's/^/    /' || echo "    (none)"
grep -oE 'wss?://[^"'"'"' ]+' "$DASH_HTML" 2>/dev/null | sort -u | head -10 | sed 's/^/    /' || echo "    (none ws)"
grep -oiE '(export|stream|bulk|download|dump|firehose|batch|ndjson|csv|websocket|sse|graphql)[a-zA-Z0-9_/-]*' "$DASH_HTML" 2>/dev/null | sort -u | head -20 | sed 's/^/    /' || echo "    (none interesting)"


###############################################################################
# 2. ALTERNATIVE PROTOCOL ENDPOINTS
###############################################################################
log_section "2. ALTERNATIVE PROTOCOL ENDPOINTS"

echo ""
log_info "--- WebSocket endpoints ---"
for ws_path in "/ws" "/websocket" "/socket" "/api/v1/ws" "/api/v1/websocket" "/api/v1/events/ws" "/socket.io/" "/sockjs-node"; do
  # Attempt HTTP upgrade handshake probe (will get a status even if not WS)
  status=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "$AUTH_HEADER" \
    -H "Upgrade: websocket" \
    -H "Connection: Upgrade" \
    -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
    -H "Sec-WebSocket-Version: 13" \
    "$BASE$ws_path" 2>/dev/null || echo "000")
  if [ "$status" = "101" ] || [ "$status" = "200" ] || [ "$status" = "400" ] || [ "$status" = "426" ]; then
    log_found "WebSocket $ws_path => HTTP $status (possible WS endpoint)"
  else
    log_miss "WebSocket $ws_path => HTTP $status"
  fi
done

echo ""
log_info "--- Server-Sent Events endpoints ---"
for sse_path in "/api/v1/events/sse" "/api/v1/events/stream" "/api/v1/sse" "/api/v1/stream" "/sse" "/stream" "/api/v1/events/live" "/api/v1/live"; do
  status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
    -H "$AUTH_HEADER" \
    -H "Accept: text/event-stream" \
    "$BASE$sse_path" 2>/dev/null || echo "000")
  if [ "$status" != "404" ] && [ "$status" != "000" ]; then
    log_found "SSE $sse_path => HTTP $status"
    # Grab a snippet
    timeout 3 curl -s -H "$AUTH_HEADER" -H "Accept: text/event-stream" "$BASE$sse_path" 2>/dev/null | head -c 500 | sed 's/^/      /' || true
    echo ""
  else
    log_miss "SSE $sse_path => HTTP $status"
  fi
done

echo ""
log_info "--- GraphQL endpoints ---"
for gql_path in "/graphql" "/api/graphql" "/api/v1/graphql" "/gql" "/api/gql"; do
  # Try introspection query
  status=$(curl -s -o "$TMPDIR/gql.tmp" -w "%{http_code}" -X POST \
    -H "$AUTH_HEADER" \
    -H "Content-Type: application/json" \
    -d '{"query":"{ __schema { types { name } } }"}' \
    "$BASE$gql_path" 2>/dev/null || echo "000")
  if [ "$status" != "404" ] && [ "$status" != "000" ]; then
    preview=$(head -c 200 "$TMPDIR/gql.tmp" 2>/dev/null | tr '\n' ' ')
    log_found "GraphQL $gql_path => HTTP $status  |  $preview"
  else
    log_miss "GraphQL $gql_path => HTTP $status"
  fi
done

echo ""
log_info "--- gRPC-web endpoints ---"
for grpc_path in "/grpc" "/api/grpc" "/grpc-web" "/api/v1/grpc"; do
  status=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    -H "$AUTH_HEADER" \
    -H "Content-Type: application/grpc-web+proto" \
    "$BASE$grpc_path" 2>/dev/null || echo "000")
  if [ "$status" != "404" ] && [ "$status" != "000" ]; then
    log_found "gRPC $grpc_path => HTTP $status"
  else
    log_miss "gRPC $grpc_path => HTTP $status"
  fi
done


###############################################################################
# 3. DEEP ENDPOINT ENUMERATION
###############################################################################
log_section "3. DEEP ENDPOINT ENUMERATION (GET)"

GET_ENDPOINTS=(
  # Events sub-paths
  "/api/v1/events"
  "/api/v1/events/export"
  "/api/v1/events/stream"
  "/api/v1/events/download"
  "/api/v1/events/bulk"
  "/api/v1/events/ids"
  "/api/v1/events/count"
  "/api/v1/events/all"
  "/api/v1/events/batch"
  "/api/v1/events/dump"
  "/api/v1/events/file"
  "/api/v1/events/csv"
  "/api/v1/events/json"
  "/api/v1/events/ndjson"
  # Top-level API v1 paths
  "/api/v1/data"
  "/api/v1/dump"
  "/api/v1/archive"
  "/api/v1/backup"
  "/api/v1/snapshot"
  "/api/v1/sync"
  "/api/v1/replicate"
  "/api/v1/firehose"
  "/api/v1/feed"
  "/api/v1/batch"
  "/api/v1/jobs"
  "/api/v1/tasks"
  "/api/v1/workers"
  "/api/v1/partitions"
  "/api/v1/segments"
  "/api/v1/chunks"
  "/api/v1/export"
  "/api/v1/download"
  "/api/v1/bulk"
  "/api/v1/status"
  "/api/v1/health"
  "/api/v1/info"
  "/api/v1/docs"
  "/api/v1/metadata"
  "/api/v1/schema"
  "/api/v1/collections"
  "/api/v1/channels"
  "/api/v1/sources"
  "/api/v1/submissions"
  # Internal / debug / admin
  "/api/internal/events"
  "/internal/events"
  "/internal/api/events"
  "/debug/events"
  "/debug/vars"
  "/debug/pprof"
  "/admin/events"
  "/admin/api"
  "/admin"
  "/api/v2/events"
  "/v1/events"
  "/v2/events"
  "/events"
)

for ep in "${GET_ENDPOINTS[@]}"; do
  probe "$BASE$ep" "GET" "$ep" || true
done

echo ""
log_section "3b. DEEP ENDPOINT ENUMERATION (POST)"

POST_ENDPOINTS=(
  "/api/v1/events"
  "/api/v1/events/export"
  "/api/v1/events/stream"
  "/api/v1/events/download"
  "/api/v1/events/bulk"
  "/api/v1/events/batch"
  "/api/v1/events/dump"
  "/api/v1/events/query"
  "/api/v1/events/search"
  "/api/v1/events/filter"
  "/api/v1/export"
  "/api/v1/download"
  "/api/v1/bulk"
  "/api/v1/dump"
  "/api/v1/query"
  "/api/v1/batch"
  "/api/v1/firehose"
  "/api/v1/sync"
  "/api/v1/data"
  "/api/v1/jobs"
  "/api/v1/tasks"
)

for ep in "${POST_ENDPOINTS[@]}"; do
  probe "$BASE$ep" "POST" "$ep" '-d {}' || true
done


###############################################################################
# 4. ROBOTS.TXT, SITEMAP, .WELL-KNOWN, OPENAPI, SWAGGER
###############################################################################
log_section "4. STANDARD DISCOVERY FILES"

DISCOVERY_URLS=(
  "/robots.txt"
  "/sitemap.xml"
  "/.well-known/security.txt"
  "/.well-known/openid-configuration"
  "/.well-known/jwks.json"
  "/.well-known/assetlinks.json"
  "/openapi.json"
  "/openapi.yaml"
  "/swagger.json"
  "/swagger.yaml"
  "/swagger-ui/"
  "/swagger-ui/index.html"
  "/api-docs"
  "/api-docs.json"
  "/docs/api.md"
  "/docs"
  "/docs/"
  "/api/v1/openapi.json"
  "/api/v1/swagger.json"
  "/api/v1/docs"
  "/api/docs"
  "/redoc"
  "/rapidoc"
)

for url in "${DISCOVERY_URLS[@]}"; do
  probe "$BASE$url" "GET" "$url" || true
done


###############################################################################
# 5. COMMON FRAMEWORK PATHS
###############################################################################
log_section "5. COMMON FRAMEWORK / INFRA PATHS"

FRAMEWORK_URLS=(
  "/health"
  "/healthz"
  "/healthcheck"
  "/ready"
  "/readyz"
  "/livez"
  "/metrics"
  "/prometheus"
  "/actuator"
  "/actuator/health"
  "/actuator/info"
  "/actuator/env"
  "/_debug"
  "/_debug/vars"
  "/debug"
  "/debug/vars"
  "/env"
  "/config"
  "/version"
  "/api/health"
  "/api/status"
  "/api/v1/ping"
  "/ping"
  "/favicon.ico"
  "/.env"
  "/.git/HEAD"
  "/server-status"
  "/server-info"
  "/__webpack_hmr"
  "/_next/data"
  "/manifest.json"
  "/asset-manifest.json"
)

for url in "${FRAMEWORK_URLS[@]}"; do
  probe "$BASE$url" "GET" "$url" || true
done


###############################################################################
# 6. ACCEPT HEADER VARIATIONS ON /api/v1/events
###############################################################################
log_section "6. CONTENT NEGOTIATION - Accept Headers on /events"

ACCEPT_TYPES=(
  "application/json"
  "text/csv"
  "application/ndjson"
  "application/x-ndjson"
  "text/plain"
  "text/tab-separated-values"
  "application/octet-stream"
  "application/x-msgpack"
  "application/protobuf"
  "application/avro"
  "application/parquet"
  "text/event-stream"
  "application/xml"
)

for accept in "${ACCEPT_TYPES[@]}"; do
  resp_file="$TMPDIR/accept_resp.tmp"
  hdr_file="$TMPDIR/accept_hdr.tmp"
  status=$(curl -s -o "$resp_file" -D "$hdr_file" -w "%{http_code}" \
    -H "$AUTH_HEADER" \
    -H "Accept: $accept" \
    "$API/events?limit=2" 2>/dev/null || echo "000")
  ct=$(grep -i 'content-type' "$hdr_file" 2>/dev/null | head -1 | tr -d '\r' || true)
  preview=$(head -c 150 "$resp_file" 2>/dev/null | tr '\n' ' ')
  if [ "$status" = "200" ]; then
    log_found "Accept: $accept => HTTP $status | $ct | ${preview:0:120}"
  else
    log_info "Accept: $accept => HTTP $status | $ct"
  fi
done


###############################################################################
# 7. ENCODING / COMPRESSION TESTS
###############################################################################
log_section "7. COMPRESSION SUPPORT"

for enc in "gzip" "deflate" "br" "zstd" "gzip, deflate, br"; do
  hdr_file="$TMPDIR/enc_hdr.tmp"
  status=$(curl -s -o /dev/null -D "$hdr_file" -w "%{http_code}" \
    -H "$AUTH_HEADER" \
    -H "Accept-Encoding: $enc" \
    "$API/events?limit=2" 2>/dev/null || echo "000")
  ce=$(grep -i 'content-encoding' "$hdr_file" 2>/dev/null | head -1 | tr -d '\r' || true)
  log_info "Accept-Encoding: $enc => HTTP $status | $ce"
done


###############################################################################
# 8. RESPONSE HEADER ANALYSIS
###############################################################################
log_section "8. FULL RESPONSE HEADERS on /api/v1/events?limit=1"

curl -s -D - -o /dev/null -H "$AUTH_HEADER" "$API/events?limit=1" 2>/dev/null | sed 's/^/    /'
echo ""

log_info "OPTIONS /api/v1/events response:"
curl -s -D - -o /dev/null -X OPTIONS -H "$AUTH_HEADER" "$API/events" 2>/dev/null | sed 's/^/    /'
echo ""

log_info "HEAD /api/v1/events response:"
curl -s -D - -o /dev/null -X HEAD -H "$AUTH_HEADER" "$API/events?limit=1" 2>/dev/null | sed 's/^/    /'
echo ""


###############################################################################
# 9. QUERY PARAMETER EXPLORATION
###############################################################################
log_section "9. QUERY PARAMETER EXPLORATION on /events"

QUERY_PARAMS=(
  "limit=1&format=csv"
  "limit=1&format=ndjson"
  "limit=1&format=json"
  "limit=1&format=raw"
  "limit=1&format=ids"
  "limit=1&output=csv"
  "limit=1&output=ndjson"
  "limit=1&fields=id"
  "limit=1&select=id"
  "limit=1&only=id"
  "limit=1&include=id"
  "limit=1&compress=true"
  "limit=1&gzip=true"
  "limit=1&stream=true"
  "limit=1&batch=true"
  "limit=1&page=1&pageSize=1"
  "limit=1&offset=0"
  "limit=1&sort=asc"
  "limit=1&sort=id"
  "limit=1&order=asc"
  "limit=1&startDate=2024-01-01"
  "limit=1&from=2024-01-01"
  "limit=1&after=0"
  "limit=1&start=0"
  "limit=1&partition=0"
  "limit=1&channel=default"
  "limit=1&source=default"
  "limit=1&type=all"
  "count=true"
  "ids_only=true"
  "limit=50000"
  "limit=100000"
)

for qp in "${QUERY_PARAMS[@]}"; do
  resp_file="$TMPDIR/qp_resp.tmp"
  status=$(curl -s -o "$resp_file" -w "%{http_code}" \
    -H "$AUTH_HEADER" \
    "$API/events?$qp" 2>/dev/null || echo "000")
  preview=$(head -c 150 "$resp_file" 2>/dev/null | tr '\n' ' ')
  if [ "$status" = "200" ]; then
    log_found "?$qp => HTTP $status | ${preview:0:120}"
  elif [ "$status" != "404" ] && [ "$status" != "000" ]; then
    log_info "?$qp => HTTP $status | ${preview:0:120}"
  else
    log_miss "?$qp => HTTP $status"
  fi
done


###############################################################################
# 10. AUTH METHOD EXPLORATION
###############################################################################
log_section "10. AUTH METHOD EXPLORATION"

# Query param auth vs header auth
log_info "Header auth (X-API-Key):"
curl -s -D "$TMPDIR/auth_hdr.tmp" -o "$TMPDIR/auth_body.tmp" \
  -H "X-API-Key: $API_KEY" "$API/events?limit=1" 2>/dev/null
grep -iE '(rate|limit|x-|authorization)' "$TMPDIR/auth_hdr.tmp" 2>/dev/null | sed 's/^/    /' || echo "    (no rate headers)"

log_info "Query param auth (?apiKey=):"
curl -s -D "$TMPDIR/auth_hdr2.tmp" -o "$TMPDIR/auth_body2.tmp" \
  "$API/events?limit=1&apiKey=$API_KEY" 2>/dev/null
grep -iE '(rate|limit|x-|authorization)' "$TMPDIR/auth_hdr2.tmp" 2>/dev/null | sed 's/^/    /' || echo "    (no rate headers)"

log_info "Bearer token auth:"
status=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $API_KEY" "$API/events?limit=1" 2>/dev/null || echo "000")
log_info "  Bearer => HTTP $status"


###############################################################################
# SUMMARY
###############################################################################
log_section "DISCOVERY COMPLETE"
echo ""
echo "  Temp files saved in: $TMPDIR"
echo "  Dashboard HTML: $DASH_HTML"
echo "  Re-run with: API_KEY=$API_KEY sh test-discovery.sh"
echo ""
echo "${BOLD}  Review [FOUND] entries above for actionable discoveries.${NC}"
echo ""
