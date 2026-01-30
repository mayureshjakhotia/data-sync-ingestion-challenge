#!/bin/bash
# =============================================================================
# Rate Limit Exploitation Test Script for DataSync API
# =============================================================================
# Tests multiple rate limit bypass strategies:
#   1. Dual auth bucket exploit (header vs query param)
#   2. Per-endpoint rate limit pools
#   3. Rate limit header analysis
#   4. HTTP method variance (HEAD, OPTIONS vs GET)
#   5. Timing / window boundary detection
#   6. Connection-level tricks (User-Agent, Keep-Alive)
#   7. API version variance (/v2, /v3)
#
# Usage:
#   API_KEY=your_key_here bash test-ratelimits.sh
# =============================================================================

set -euo pipefail

BASE="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API_V1="$BASE/api/v1"

if [ -z "${API_KEY:-}" ]; then
  echo "ERROR: API_KEY environment variable is required."
  echo "Usage: API_KEY=your_key bash test-ratelimits.sh"
  exit 1
fi

TMPDIR_RL=$(mktemp -d)
trap 'rm -rf "$TMPDIR_RL"' EXIT

# Color helpers (degrade gracefully if no tty)
if [ -t 1 ]; then
  BOLD="\033[1m"
  GREEN="\033[32m"
  YELLOW="\033[33m"
  RED="\033[31m"
  CYAN="\033[36m"
  RESET="\033[0m"
else
  BOLD="" GREEN="" YELLOW="" RED="" CYAN="" RESET=""
fi

banner() { echo -e "\n${BOLD}${CYAN}== $1 ==${RESET}"; }
info()   { echo -e "  ${GREEN}[INFO]${RESET} $1"; }
warn()   { echo -e "  ${YELLOW}[WARN]${RESET} $1"; }
fail()   { echo -e "  ${RED}[FAIL]${RESET} $1"; }
detail() { echo -e "  $1"; }

# ---------------------------------------------------------------------------
# Utility: make a request, capture headers + body + status
#   req METHOD URL [extra_curl_args...]
#   Outputs to: $TMPDIR_RL/last_headers, $TMPDIR_RL/last_body
#   Returns HTTP status code as exit-value-friendly text on stdout
# ---------------------------------------------------------------------------
req() {
  local method="$1"; shift
  local url="$1"; shift
  curl -s -o "$TMPDIR_RL/last_body" \
       -D "$TMPDIR_RL/last_headers" \
       -w "%{http_code}" \
       -X "$method" \
       "$@" \
       "$url"
}

# Extract a header value (case-insensitive) from last_headers
get_header() {
  grep -i "^$1:" "$TMPDIR_RL/last_headers" 2>/dev/null | sed 's/^[^:]*: *//' | tr -d '\r' || true
}

# ---------------------------------------------------------------------------
echo -e "${BOLD}=============================================${RESET}"
echo -e "${BOLD}  Rate Limit Exploitation Test Suite${RESET}"
echo -e "${BOLD}=============================================${RESET}"
echo "  Target: $API_V1"
echo "  Key:    ${API_KEY:0:8}...${API_KEY: -4}"
echo "  Time:   $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo ""

# =====================================================================
# TEST 1: Full Header Analysis -- Header Auth vs Query Param Auth
# =====================================================================
banner "TEST 1: Rate Limit Header Analysis (Header Auth vs Query Param Auth)"

info "Request via X-API-Key header..."
STATUS_H=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
cp "$TMPDIR_RL/last_headers" "$TMPDIR_RL/headers_header_auth"
info "HTTP $STATUS_H"

echo ""
info "ALL response headers (header auth):"
while IFS= read -r line; do
  line="${line//$'\r'/}"
  [ -z "$line" ] && continue
  detail "  $line"
done < "$TMPDIR_RL/headers_header_auth"

# Capture specific rate limit headers
RL_LIMIT_H=$(grep -i "ratelimit\|rate-limit\|x-rate\|retry-after" "$TMPDIR_RL/headers_header_auth" 2>/dev/null | tr -d '\r' || echo "(none found)")
echo ""
info "Rate-limit related headers (header auth):"
detail "  $RL_LIMIT_H"

echo ""
info "Request via ?apiKey= query param..."
STATUS_Q=$(req GET "$API_V1/events?limit=1&apiKey=$API_KEY")
cp "$TMPDIR_RL/last_headers" "$TMPDIR_RL/headers_query_auth"
info "HTTP $STATUS_Q"

echo ""
info "ALL response headers (query param auth):"
while IFS= read -r line; do
  line="${line//$'\r'/}"
  [ -z "$line" ] && continue
  detail "  $line"
done < "$TMPDIR_RL/headers_query_auth"

RL_LIMIT_Q=$(grep -i "ratelimit\|rate-limit\|x-rate\|retry-after" "$TMPDIR_RL/headers_query_auth" 2>/dev/null | tr -d '\r' || echo "(none found)")
echo ""
info "Rate-limit related headers (query param auth):"
detail "  $RL_LIMIT_Q"

# Compare
echo ""
info "Comparing rate limit headers between auth methods..."
DIFF_RESULT=$(diff <(grep -i "ratelimit\|rate-limit\|x-rate\|retry" "$TMPDIR_RL/headers_header_auth" 2>/dev/null | sort || true) \
                   <(grep -i "ratelimit\|rate-limit\|x-rate\|retry" "$TMPDIR_RL/headers_query_auth" 2>/dev/null | sort || true) || true)
if [ -z "$DIFF_RESULT" ]; then
  info "Rate limit headers are IDENTICAL between auth methods."
else
  warn "Rate limit headers DIFFER between auth methods:"
  detail "$DIFF_RESULT"
fi

# =====================================================================
# TEST 2: Dual Auth Bucket Exploit
# =====================================================================
banner "TEST 2: Dual Auth Bucket Exploit (Separate Rate Limit Pools?)"

info "Sending 5 rapid requests with HEADER auth..."
declare -a REMAINING_H=()
for i in $(seq 1 5); do
  STATUS=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
  REMAINING=$(get_header "X-RateLimit-Remaining")
  REMAINING_ALT=$(get_header "RateLimit-Remaining")
  R="${REMAINING:-${REMAINING_ALT:-(n/a)}}"
  REMAINING_H+=("$R")
  detail "  Request $i: HTTP $STATUS | Remaining: $R"
  # Tiny sleep to avoid self-DOS
  sleep 0.05
done

echo ""
info "Sending 5 rapid requests with QUERY PARAM auth..."
declare -a REMAINING_Q=()
for i in $(seq 1 5); do
  STATUS=$(req GET "$API_V1/events?limit=1&apiKey=$API_KEY")
  REMAINING=$(get_header "X-RateLimit-Remaining")
  REMAINING_ALT=$(get_header "RateLimit-Remaining")
  R="${REMAINING:-${REMAINING_ALT:-(n/a)}}"
  REMAINING_Q+=("$R")
  detail "  Request $i: HTTP $STATUS | Remaining: $R"
  sleep 0.05
done

echo ""
info "Header auth remaining sequence:      ${REMAINING_H[*]}"
info "Query param auth remaining sequence:  ${REMAINING_Q[*]}"

# Check if the sequences suggest separate pools
if [ "${REMAINING_H[0]}" != "(n/a)" ] && [ "${REMAINING_Q[0]}" != "(n/a)" ]; then
  if [ "${REMAINING_Q[0]}" -ge "${REMAINING_H[0]}" ] 2>/dev/null; then
    warn "Query param pool appears to have SEPARATE (or reset) counters -- exploit viable!"
  else
    info "Counters appear shared or query has fewer limits."
  fi
else
  info "Could not parse remaining counts. Check raw headers above for clues."
fi

# Now test: exhaust header auth further, then check if query param still works
echo ""
info "Sending 20 more rapid requests with HEADER auth to push toward limit..."
LAST_STATUS_H=""
for i in $(seq 1 20); do
  LAST_STATUS_H=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
  sleep 0.02
done
info "Last header-auth status after 20 more: HTTP $LAST_STATUS_H"
REMAINING_AFTER_H=$(get_header "X-RateLimit-Remaining")
info "Header auth remaining after burst: ${REMAINING_AFTER_H:-(n/a)}"

info "Now testing query param auth (should still have quota if separate pools)..."
STATUS_AFTER_Q=$(req GET "$API_V1/events?limit=1&apiKey=$API_KEY")
REMAINING_AFTER_Q=$(get_header "X-RateLimit-Remaining")
info "Query param auth status: HTTP $STATUS_AFTER_Q | Remaining: ${REMAINING_AFTER_Q:-(n/a)}"

if [ "$STATUS_AFTER_Q" = "200" ] && [ "$LAST_STATUS_H" = "429" ]; then
  warn "CONFIRMED: Separate rate limit pools! Header exhausted (429) but query param still works (200)."
elif [ "$LAST_STATUS_H" = "429" ] && [ "$STATUS_AFTER_Q" = "429" ]; then
  info "Both exhausted -- pools are likely shared."
else
  info "Neither exhausted yet (both returned 200). Need more requests to fully test."
fi

# =====================================================================
# TEST 3: Per-Endpoint Rate Limit Pools
# =====================================================================
banner "TEST 3: Per-Endpoint Rate Limit Pools"

ENDPOINTS_TO_TEST=(
  "/events"
  "/events/export"
  "/events/stream"
  "/events/download"
  "/events/bulk"
  "/events/count"
  "/events/ids"
  "/events/all"
  "/export"
  "/stream"
  "/status"
  "/health"
)

for ep in "${ENDPOINTS_TO_TEST[@]}"; do
  STATUS=$(req GET "$API_V1${ep}?limit=1" -H "X-API-Key: $API_KEY")
  LIMIT=$(get_header "X-RateLimit-Limit")
  REMAINING=$(get_header "X-RateLimit-Remaining")
  BUCKET=$(get_header "X-RateLimit-Bucket")
  LIMIT_ALT=$(get_header "RateLimit-Limit")
  REMAINING_ALT=$(get_header "RateLimit-Remaining")

  L="${LIMIT:-${LIMIT_ALT:--}}"
  R="${REMAINING:-${REMAINING_ALT:--}}"
  B="${BUCKET:--}"

  if [ "$STATUS" != "404" ] && [ "$STATUS" != "000" ]; then
    info "$ep => HTTP $STATUS | Limit: $L | Remaining: $R | Bucket: $B"
  else
    detail "  $ep => HTTP $STATUS (not found, skipping)"
  fi
  sleep 0.05
done

# =====================================================================
# TEST 4: HTTP Method Variance
# =====================================================================
banner "TEST 4: HTTP Method Variance (HEAD / OPTIONS / GET)"

for METHOD in GET HEAD OPTIONS; do
  STATUS=$(req "$METHOD" "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
  LIMIT=$(get_header "X-RateLimit-Limit")
  REMAINING=$(get_header "X-RateLimit-Remaining")
  LIMIT_ALT=$(get_header "RateLimit-Limit")
  REMAINING_ALT=$(get_header "RateLimit-Remaining")
  L="${LIMIT:-${LIMIT_ALT:--}}"
  R="${REMAINING:-${REMAINING_ALT:--}}"
  info "$METHOD /events => HTTP $STATUS | Limit: $L | Remaining: $R"

  # Show all headers for non-GET
  if [ "$METHOD" != "GET" ]; then
    detail "    Headers:"
    while IFS= read -r line; do
      line="${line//$'\r'/}"
      [ -z "$line" ] && continue
      detail "      $line"
    done < "$TMPDIR_RL/last_headers"
  fi
  sleep 0.1
done

echo ""
info "Testing if HEAD/OPTIONS decrement the same counter as GET..."
info "Sending 5x GET, then 5x HEAD, then 5x GET, comparing remaining..."
for i in $(seq 1 5); do
  req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY" > /dev/null
  sleep 0.02
done
STATUS_MID=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
R_AFTER_GET=$(get_header "X-RateLimit-Remaining")
R_AFTER_GET="${R_AFTER_GET:-$(get_header "RateLimit-Remaining")}"
info "After 5 GETs: Remaining = ${R_AFTER_GET:-(n/a)}"

for i in $(seq 1 5); do
  req HEAD "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY" > /dev/null
  sleep 0.02
done
STATUS_MID2=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
R_AFTER_HEAD=$(get_header "X-RateLimit-Remaining")
R_AFTER_HEAD="${R_AFTER_HEAD:-$(get_header "RateLimit-Remaining")}"
info "After 5 HEADs: Remaining = ${R_AFTER_HEAD:-(n/a)}"

if [ "${R_AFTER_GET}" != "(n/a)" ] && [ "${R_AFTER_HEAD}" != "(n/a)" ]; then
  DIFF=$((${R_AFTER_GET:-0} - ${R_AFTER_HEAD:-0}))
  if [ "$DIFF" -gt 3 ] 2>/dev/null; then
    info "HEAD requests DO appear to decrement the counter (dropped by $DIFF)."
  elif [ "$DIFF" -le 1 ] 2>/dev/null; then
    warn "HEAD requests may NOT count against GET limits -- exploit viable!"
  else
    info "Ambiguous result (diff=$DIFF). Needs more data points."
  fi
fi

# =====================================================================
# TEST 5: Timing / Window Boundary Detection
# =====================================================================
banner "TEST 5: Rate Limit Window Timing Analysis"

STATUS=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
RESET=$(get_header "X-RateLimit-Reset")
RESET_ALT=$(get_header "RateLimit-Reset")
RETRY=$(get_header "Retry-After")
LIMIT=$(get_header "X-RateLimit-Limit")
REMAINING=$(get_header "X-RateLimit-Remaining")
LIMIT_ALT=$(get_header "RateLimit-Limit")
REMAINING_ALT=$(get_header "RateLimit-Remaining")

L="${LIMIT:-${LIMIT_ALT:-(n/a)}}"
R="${REMAINING:-${REMAINING_ALT:-(n/a)}}"
RST="${RESET:-${RESET_ALT:-(n/a)}}"
RT="${RETRY:-(n/a)}"

info "Current state:"
detail "  Limit:     $L"
detail "  Remaining: $R"
detail "  Reset:     $RST"
detail "  Retry:     $RT"

if [ "$RST" != "(n/a)" ]; then
  NOW=$(date +%s)
  # Reset might be epoch seconds or seconds-until-reset
  if [ "$RST" -gt 1000000000 ] 2>/dev/null; then
    SECS_LEFT=$((RST - NOW))
    info "Reset is epoch timestamp. Window resets in ~${SECS_LEFT}s (at $(date -r "$RST" '+%H:%M:%S' 2>/dev/null || echo "$RST"))"
    info "EXPLOIT: Burst requests, wait ${SECS_LEFT}s, burst again at boundary."
  elif [ "$RST" -gt 0 ] 2>/dev/null; then
    info "Reset is relative: ${RST}s from now."
    info "EXPLOIT: Burst requests, wait ${RST}s, burst again."
  fi
fi

# Measure window by watching remaining count change
echo ""
info "Measuring rate limit window by polling every 2s for 20s..."
for i in $(seq 1 10); do
  STATUS=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY")
  R=$(get_header "X-RateLimit-Remaining")
  R="${R:-$(get_header "RateLimit-Remaining")}"
  RST=$(get_header "X-RateLimit-Reset")
  RST="${RST:-$(get_header "RateLimit-Reset")}"
  TS=$(date '+%H:%M:%S')
  detail "  [$TS] HTTP $STATUS | Remaining: ${R:--} | Reset: ${RST:--}"
  if [ "$STATUS" = "429" ]; then
    warn "Hit 429! Retry-After: $(get_header "Retry-After")"
    break
  fi
  sleep 2
done

# =====================================================================
# TEST 6: Connection-Level Tricks
# =====================================================================
banner "TEST 6: Connection-Level Tricks"

info "Testing different User-Agent strings..."
AGENTS=(
  "Mozilla/5.0 (compatible; DataSyncBot/1.0)"
  "curl/8.0"
  "python-requests/2.31"
  "Go-http-client/2.0"
  "node-fetch/3.0"
)

for UA in "${AGENTS[@]}"; do
  STATUS=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY" -H "User-Agent: $UA")
  R=$(get_header "X-RateLimit-Remaining")
  R="${R:-$(get_header "RateLimit-Remaining")}"
  detail "  UA: '$UA' => HTTP $STATUS | Remaining: ${R:--}"
  sleep 0.05
done

echo ""
info "Testing Connection: close vs keep-alive..."
for CONN in "close" "keep-alive"; do
  STATUS=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY" -H "Connection: $CONN")
  R=$(get_header "X-RateLimit-Remaining")
  R="${R:-$(get_header "RateLimit-Remaining")}"
  detail "  Connection: $CONN => HTTP $STATUS | Remaining: ${R:--}"
  sleep 0.05
done

echo ""
info "Testing HTTP/1.0 vs HTTP/1.1 vs HTTP/2..."
for VER in --http1.0 --http1.1 --http2; do
  STATUS=$(req GET "$API_V1/events?limit=1" -H "X-API-Key: $API_KEY" "$VER" 2>/dev/null || echo "err")
  R=$(get_header "X-RateLimit-Remaining")
  R="${R:-$(get_header "RateLimit-Remaining")}"
  detail "  $VER => HTTP $STATUS | Remaining: ${R:--}"
  sleep 0.05
done

# =====================================================================
# TEST 7: API Version Variance
# =====================================================================
banner "TEST 7: API Version Variance"

for VER in v1 v2 v3 v0 v1.1; do
  STATUS=$(req GET "$BASE/api/$VER/events?limit=1" -H "X-API-Key: $API_KEY")
  R=$(get_header "X-RateLimit-Remaining")
  R="${R:-$(get_header "RateLimit-Remaining")}"
  L=$(get_header "X-RateLimit-Limit")
  L="${L:-$(get_header "RateLimit-Limit")}"

  if [ "$STATUS" != "404" ] && [ "$STATUS" != "000" ]; then
    warn "/api/$VER/events => HTTP $STATUS | Limit: ${L:--} | Remaining: ${R:--}"
    detail "    Body preview: $(head -c 200 "$TMPDIR_RL/last_body" 2>/dev/null)"
  else
    detail "  /api/$VER/events => HTTP $STATUS (not found)"
  fi
  sleep 0.05
done

# Also try without version prefix
STATUS=$(req GET "$BASE/events?limit=1" -H "X-API-Key: $API_KEY")
if [ "$STATUS" != "404" ] && [ "$STATUS" != "000" ]; then
  warn "/events (no version) => HTTP $STATUS"
fi

# =====================================================================
# TEST 8: Parallel Dual-Auth Burst
# =====================================================================
banner "TEST 8: Parallel Dual-Auth Burst (Maximize Throughput)"

info "Firing 10 requests: 5 header-auth + 5 query-auth in parallel..."
mkdir -p "$TMPDIR_RL/parallel"

for i in $(seq 1 5); do
  curl -s -D "$TMPDIR_RL/parallel/h_headers_$i" -o "$TMPDIR_RL/parallel/h_body_$i" \
       -w "HEADER_$i:%{http_code}\n" \
       -H "X-API-Key: $API_KEY" "$API_V1/events?limit=1" &
  curl -s -D "$TMPDIR_RL/parallel/q_headers_$i" -o "$TMPDIR_RL/parallel/q_body_$i" \
       -w "QUERY_$i:%{http_code}\n" \
       "$API_V1/events?limit=1&apiKey=$API_KEY" &
done
wait

echo ""
info "Results from parallel burst:"
for i in $(seq 1 5); do
  H_RL=$(grep -i "x-ratelimit-remaining\|ratelimit-remaining" "$TMPDIR_RL/parallel/h_headers_$i" 2>/dev/null | tr -d '\r' || echo "(none)")
  Q_RL=$(grep -i "x-ratelimit-remaining\|ratelimit-remaining" "$TMPDIR_RL/parallel/q_headers_$i" 2>/dev/null | tr -d '\r' || echo "(none)")
  H_ST=$(grep -i "^HTTP/" "$TMPDIR_RL/parallel/h_headers_$i" 2>/dev/null | tail -1 | tr -d '\r' || echo "?")
  Q_ST=$(grep -i "^HTTP/" "$TMPDIR_RL/parallel/q_headers_$i" 2>/dev/null | tail -1 | tr -d '\r' || echo "?")
  detail "  Pair $i: Header[$H_ST | $H_RL] -- Query[$Q_ST | $Q_RL]"
done

# =====================================================================
# SUMMARY
# =====================================================================
banner "SUMMARY & RECOMMENDATIONS"

echo ""
info "Data collected. Key findings:"
echo ""
detail "  1. Auth Methods: Header returned HTTP $STATUS_H, Query returned HTTP $STATUS_Q"
detail "  2. Rate limit headers found: $RL_LIMIT_H"
detail "  3. Check per-endpoint results above for separate bucket indicators"
detail "  4. Check HTTP method results for HEAD/OPTIONS exemptions"
detail "  5. Check version results for unprotected API versions"
echo ""
info "If separate buckets are confirmed, interleave header-auth and query-auth"
info "requests to effectively double throughput."
info "If HEAD/OPTIONS are exempt, use them for polling/metadata without cost."
info "If /v2 or /v3 exist with different limits, use them as additional pools."
echo ""
echo -e "${BOLD}=============================================${RESET}"
echo -e "${BOLD}  Test Complete: $(date -u '+%Y-%m-%dT%H:%M:%SZ')${RESET}"
echo -e "${BOLD}=============================================${RESET}"
