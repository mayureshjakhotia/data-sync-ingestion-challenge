#!/usr/bin/env bash
###############################################################################
# test-payload.sh
#
# Payload size & transfer speed optimization probe for the DataSync API.
# Tests field selection, compression, alternative formats, conditional
# requests, HTTP/2, chunked transfer, and range requests.
#
# Usage:
#   API_KEY=your_key_here bash test-payload.sh
#
# Requirements: curl (7.68+ recommended for --http2), python3, bc
###############################################################################

set -euo pipefail

BASE="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API="$BASE/api/v1"
LIMIT=100                 # events per request for comparison tests
TMPDIR_PREFIX="/tmp/payload-test"
RESULTS_FILE="/tmp/payload-test-results.txt"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
if [ -z "${API_KEY:-}" ]; then
  echo "ERROR: API_KEY env var is required."
  echo "Usage: API_KEY=your_key bash test-payload.sh"
  exit 1
fi

mkdir -p "$TMPDIR_PREFIX"
: > "$RESULTS_FILE"       # truncate results file

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
header() {
  echo ""
  echo "============================================================"
  echo "  $1"
  echo "============================================================"
}

sub_header() {
  echo ""
  echo "--- $1 ---"
}

# Perform a curl request, capture headers + body + timing, print summary.
# Usage: probe LABEL [extra_curl_args...]
# Writes body to $TMPDIR_PREFIX/body, headers to $TMPDIR_PREFIX/headers
probe() {
  local label="$1"; shift
  local hdr_file="$TMPDIR_PREFIX/headers"
  local body_file="$TMPDIR_PREFIX/body"

  local http_code
  http_code=$(curl -s -w '%{http_code}' \
    -D "$hdr_file" \
    -o "$body_file" \
    -H "X-API-Key: $API_KEY" \
    --max-time 30 \
    "$@" 2>/dev/null || echo "000")

  local raw_size
  raw_size=$(wc -c < "$body_file" | tr -d ' ')

  local content_type
  content_type=$(grep -i '^content-type:' "$hdr_file" 2>/dev/null | head -1 | tr -d '\r' || echo "(none)")

  local content_encoding
  content_encoding=$(grep -i '^content-encoding:' "$hdr_file" 2>/dev/null | head -1 | tr -d '\r' || echo "(none)")

  printf "  %-55s  HTTP %-4s  %8s bytes  %s  %s\n" \
    "$label" "$http_code" "$raw_size" "$content_type" "$content_encoding"

  # Log to results file for final comparison
  echo "$raw_size|$http_code|$label" >> "$RESULTS_FILE"
}

# Same as probe but also prints the first N bytes of the body.
probe_peek() {
  local label="$1"; shift
  local peek_lines="${1:-3}"; shift
  probe "$label" "$@"
  echo "    Body preview:"
  head -"$peek_lines" "$TMPDIR_PREFIX/body" 2>/dev/null | sed 's/^/    | /'
  echo ""
}

###############################################################################
# 1. BASELINE
###############################################################################
header "1. BASELINE (no special headers, limit=$LIMIT)"

probe_peek "baseline (JSON, no compression)" 3 \
  "$API/events?limit=$LIMIT"

BASELINE_SIZE=$(wc -c < "$TMPDIR_PREFIX/body" | tr -d ' ')
echo "  >>> Baseline body size: $BASELINE_SIZE bytes"

###############################################################################
# 2. FIELD SELECTION / SPARSE FIELDSETS
###############################################################################
header "2. FIELD SELECTION / SPARSE FIELDSETS"
echo "  Goal: Request only 'id' field to shrink payload."
echo ""

FIELD_PARAMS=(
  "fields=id"
  "select=id"
  "only=id"
  "include=id"
  "columns=id"
  "props=id"
  "projection=id"
  "fields=id,type"
  "select=id,type"
  "fields[events]=id"
  "filter[fields]=id"
)

for param in "${FIELD_PARAMS[@]}"; do
  probe_peek "?$param" 2 \
    "$API/events?limit=$LIMIT&$param"
done

sub_header "Field selection via custom headers"

FIELD_HEADERS=(
  "X-Fields: id"
  "X-Select: id"
  "X-Only: id"
  "X-Columns: id"
  "X-Projection: id"
  "X-Include: id"
)

for hval in "${FIELD_HEADERS[@]}"; do
  probe_peek "Header: $hval" 2 \
    -H "$hval" "$API/events?limit=$LIMIT"
done

###############################################################################
# 3. RESPONSE COMPRESSION
###############################################################################
header "3. RESPONSE COMPRESSION"
echo "  Comparing response sizes with various Accept-Encoding values."
echo "  (curl --compressed will auto-decompress; we use raw to measure wire size)"
echo ""

# Fetch WITHOUT decompression to measure actual wire bytes
ENCODINGS=(
  "gzip"
  "br"
  "deflate"
  "zstd"
  "gzip, deflate, br"
  "identity"
)

for enc in "${ENCODINGS[@]}"; do
  # Use --raw so curl does NOT decompress
  probe "Accept-Encoding: $enc" \
    -H "Accept-Encoding: $enc" \
    --raw \
    "$API/events?limit=$LIMIT"
done

# Also test curl's --compressed shorthand (auto-decompresses)
sub_header "curl --compressed (auto-decompresses)"
probe "curl --compressed (decompressed size)" \
  --compressed \
  "$API/events?limit=$LIMIT"

# Calculate compression ratio for gzip
sub_header "Compression ratio analysis"
echo "  Fetching raw (identity) vs gzip for limit=1000..."

curl -s -o "$TMPDIR_PREFIX/raw_1000" \
  -H "X-API-Key: $API_KEY" \
  -H "Accept-Encoding: identity" \
  "$API/events?limit=1000" 2>/dev/null || true

curl -s -o "$TMPDIR_PREFIX/gzip_1000" \
  -H "X-API-Key: $API_KEY" \
  -H "Accept-Encoding: gzip" \
  --raw \
  "$API/events?limit=1000" 2>/dev/null || true

RAW_1K=$(wc -c < "$TMPDIR_PREFIX/raw_1000" 2>/dev/null | tr -d ' ')
GZIP_1K=$(wc -c < "$TMPDIR_PREFIX/gzip_1000" 2>/dev/null | tr -d ' ')

if [ "$RAW_1K" -gt 0 ] 2>/dev/null && [ "$GZIP_1K" -gt 0 ] 2>/dev/null; then
  RATIO=$(python3 -c "print(f'{($GZIP_1K/$RAW_1K)*100:.1f}%')" 2>/dev/null || echo "N/A")
  echo "  Raw (limit=1000):  $RAW_1K bytes"
  echo "  Gzip (limit=1000): $GZIP_1K bytes"
  echo "  Gzip is $RATIO of raw size"
  SAVED=$(python3 -c "print(f'{(1 - $GZIP_1K/$RAW_1K)*100:.1f}%')" 2>/dev/null || echo "N/A")
  echo "  Savings: $SAVED"
else
  echo "  Could not compute ratio (one or both fetches failed)."
fi

###############################################################################
# 4. ALTERNATIVE RESPONSE FORMATS
###############################################################################
header "4. ALTERNATIVE RESPONSE FORMATS"
echo "  Testing Accept header to request non-JSON formats."
echo ""

ACCEPT_TYPES=(
  "text/csv"
  "application/ndjson"
  "application/x-ndjson"
  "text/plain"
  "application/msgpack"
  "application/x-msgpack"
  "application/protobuf"
  "application/x-protobuf"
  "application/cbor"
  "text/tsv"
  "text/tab-separated-values"
  "application/octet-stream"
  "application/xml"
  "text/xml"
  "application/yaml"
  "text/yaml"
)

for accept in "${ACCEPT_TYPES[@]}"; do
  probe_peek "Accept: $accept" 2 \
    -H "Accept: $accept" "$API/events?limit=$LIMIT"
done

###############################################################################
# 5. CONDITIONAL REQUESTS (ETag / Last-Modified)
###############################################################################
header "5. CONDITIONAL REQUESTS"

sub_header "Checking for ETag and Last-Modified in response"
curl -s -D "$TMPDIR_PREFIX/cond_headers" -o /dev/null \
  -H "X-API-Key: $API_KEY" \
  "$API/events?limit=10" 2>/dev/null || true

echo "  Relevant headers from GET /events?limit=10:"
grep -iE '^(etag|last-modified|cache-control|expires|vary|age):' \
  "$TMPDIR_PREFIX/cond_headers" 2>/dev/null | sed 's/^/    /' || echo "    (none found)"

ETAG=$(grep -i '^etag:' "$TMPDIR_PREFIX/cond_headers" 2>/dev/null | head -1 | sed 's/^[^:]*: *//' | tr -d '\r' || true)
LAST_MOD=$(grep -i '^last-modified:' "$TMPDIR_PREFIX/cond_headers" 2>/dev/null | head -1 | sed 's/^[^:]*: *//' | tr -d '\r' || true)

if [ -n "$ETAG" ]; then
  sub_header "Testing If-None-Match with ETag: $ETAG"
  probe "If-None-Match: $ETAG" \
    -H "If-None-Match: $ETAG" "$API/events?limit=10"
else
  echo "  No ETag header found -- skipping If-None-Match test."
fi

if [ -n "$LAST_MOD" ]; then
  sub_header "Testing If-Modified-Since: $LAST_MOD"
  probe "If-Modified-Since: $LAST_MOD" \
    -H "If-Modified-Since: $LAST_MOD" "$API/events?limit=10"
else
  echo "  No Last-Modified header found -- skipping If-Modified-Since test."
fi

###############################################################################
# 6. HTTP/2 SUPPORT
###############################################################################
header "6. HTTP/2 SUPPORT"

sub_header "Testing with --http2"
# Note: HTTP/2 over cleartext (h2c) requires --http2 flag
HTTP2_CODE=$(curl -s -o "$TMPDIR_PREFIX/h2body" -w '%{http_code}\n%{http_version}' \
  --http2 \
  -H "X-API-Key: $API_KEY" \
  "$API/events?limit=10" 2>/dev/null || echo "000\n0")

HTTP2_VER=$(echo "$HTTP2_CODE" | tail -1)
HTTP2_STATUS=$(echo "$HTTP2_CODE" | head -1)
echo "  HTTP version negotiated: $HTTP2_VER"
echo "  Status code: $HTTP2_STATUS"

if [ "$HTTP2_VER" = "2" ] || [ "$HTTP2_VER" = "2.0" ]; then
  echo "  HTTP/2 IS supported."
else
  echo "  HTTP/2 NOT supported (got HTTP/$HTTP2_VER). Server uses HTTP/1.1."
fi

sub_header "Testing with --http2-prior-knowledge (h2c upgrade)"
H2C_CODE=$(curl -s -o /dev/null -w '%{http_code}\n%{http_version}' \
  --http2-prior-knowledge \
  -H "X-API-Key: $API_KEY" \
  "$API/events?limit=10" 2>/dev/null || echo "000\n0")

H2C_VER=$(echo "$H2C_CODE" | tail -1)
H2C_STATUS=$(echo "$H2C_CODE" | head -1)
echo "  h2c version: $H2C_VER  status: $H2C_STATUS"

###############################################################################
# 7. TRANSFER-ENCODING & CONNECTION BEHAVIOR
###############################################################################
header "7. TRANSFER-ENCODING & CONNECTION BEHAVIOR"

sub_header "Checking Transfer-Encoding and Connection headers"
curl -s -D "$TMPDIR_PREFIX/te_headers" -o /dev/null \
  -H "X-API-Key: $API_KEY" \
  "$API/events?limit=1000" 2>/dev/null || true

echo "  Transfer-related headers (limit=1000):"
grep -iE '^(transfer-encoding|connection|keep-alive|content-length):' \
  "$TMPDIR_PREFIX/te_headers" 2>/dev/null | sed 's/^/    /' || echo "    (none found)"

IS_CHUNKED=$(grep -ci 'transfer-encoding:.*chunked' "$TMPDIR_PREFIX/te_headers" 2>/dev/null || echo "0")
if [ "$IS_CHUNKED" -gt 0 ]; then
  echo "  Server uses chunked transfer encoding (streaming-capable)."
else
  echo "  Server does NOT use chunked transfer encoding."
fi

sub_header "Testing TE: chunked request header"
probe "TE: chunked" \
  -H "TE: chunked" "$API/events?limit=$LIMIT"

###############################################################################
# 8. RANGE REQUESTS
###############################################################################
header "8. RANGE REQUESTS"

sub_header "Testing Range: bytes=0-9999"
RANGE_CODE=$(curl -s -o "$TMPDIR_PREFIX/range_body" -w '%{http_code}' \
  -D "$TMPDIR_PREFIX/range_headers" \
  -H "X-API-Key: $API_KEY" \
  -H "Range: bytes=0-9999" \
  "$API/events?limit=$LIMIT" 2>/dev/null || echo "000")

echo "  Status: HTTP $RANGE_CODE"
if [ "$RANGE_CODE" = "206" ]; then
  echo "  Partial content IS supported (HTTP 206)."
  grep -iE '^(content-range|accept-ranges):' "$TMPDIR_PREFIX/range_headers" 2>/dev/null | sed 's/^/    /' || true
elif [ "$RANGE_CODE" = "200" ]; then
  echo "  Server returned full response (HTTP 200) -- Range not honored."
  ACCEPT_RANGES=$(grep -i '^accept-ranges:' "$TMPDIR_PREFIX/range_headers" 2>/dev/null | tr -d '\r' || true)
  echo "  $ACCEPT_RANGES"
else
  echo "  Unexpected status. Server may not support range requests."
fi

###############################################################################
# 9. LARGE PAGE SIZE WITH COMPRESSION (combined optimization)
###############################################################################
header "9. COMBINED OPTIMIZATION: Large page + gzip"

for COMBINED_LIMIT in 1000 5000 10000; do
  sub_header "limit=$COMBINED_LIMIT"

  # Raw
  curl -s -o "$TMPDIR_PREFIX/combined_raw_$COMBINED_LIMIT" \
    -H "X-API-Key: $API_KEY" \
    -H "Accept-Encoding: identity" \
    "$API/events?limit=$COMBINED_LIMIT" 2>/dev/null || true

  # Gzip (raw wire bytes)
  curl -s -o "$TMPDIR_PREFIX/combined_gz_$COMBINED_LIMIT" \
    -H "X-API-Key: $API_KEY" \
    -H "Accept-Encoding: gzip" \
    --raw \
    "$API/events?limit=$COMBINED_LIMIT" 2>/dev/null || true

  R_SIZE=$(wc -c < "$TMPDIR_PREFIX/combined_raw_$COMBINED_LIMIT" 2>/dev/null | tr -d ' ')
  G_SIZE=$(wc -c < "$TMPDIR_PREFIX/combined_gz_$COMBINED_LIMIT" 2>/dev/null | tr -d ' ')

  # Count events returned
  EVENTS_RETURNED=$(python3 -c "
import json, sys
try:
    d = json.load(open('$TMPDIR_PREFIX/combined_raw_$COMBINED_LIMIT'))
    print(len(d.get('data', [])))
except: print('?')
" 2>/dev/null || echo "?")

  echo "  Events returned: $EVENTS_RETURNED"
  echo "  Raw size:  $R_SIZE bytes"
  echo "  Gzip size: $G_SIZE bytes"

  if [ "$R_SIZE" -gt 0 ] 2>/dev/null && [ "$G_SIZE" -gt 0 ] 2>/dev/null; then
    PER_EVENT_RAW=$(python3 -c "
import json
try:
    d = json.load(open('$TMPDIR_PREFIX/combined_raw_$COMBINED_LIMIT'))
    n = len(d.get('data', []))
    print(f'{$R_SIZE/n:.1f}') if n > 0 else print('N/A')
except: print('N/A')
" 2>/dev/null || echo "N/A")

    PER_EVENT_GZ=$(python3 -c "
import json
try:
    d = json.load(open('$TMPDIR_PREFIX/combined_raw_$COMBINED_LIMIT'))
    n = len(d.get('data', []))
    print(f'{$G_SIZE/n:.1f}') if n > 0 else print('N/A')
except: print('N/A')
" 2>/dev/null || echo "N/A")

    echo "  Bytes/event (raw):  $PER_EVENT_RAW"
    echo "  Bytes/event (gzip): $PER_EVENT_GZ"
  fi
done

###############################################################################
# 10. EVENT STRUCTURE ANALYSIS
###############################################################################
header "10. EVENT STRUCTURE ANALYSIS"

echo "  Analyzing a single event to understand field sizes..."
EVENT_JSON=$(curl -s -H "X-API-Key: $API_KEY" "$API/events?limit=1" 2>/dev/null)
python3 -c "
import json, sys

try:
    resp = json.loads('''$( echo "$EVENT_JSON" | sed "s/'/\\\\'/g" )''')
except:
    try:
        resp = json.load(sys.stdin)
    except:
        print('  Could not parse event JSON')
        sys.exit(0)

data = resp.get('data', [])
if not data:
    print('  No events in response')
    sys.exit(0)

event = data[0]
print(f'  Number of fields per event: {len(event)}')
print(f'  Fields: {list(event.keys())}')
print()

total = len(json.dumps(event))
print(f'  Total event JSON size: {total} bytes')
print()

for key, val in event.items():
    field_size = len(json.dumps(val))
    pct = (field_size / total) * 100
    print(f'    {key:25s}  {field_size:6d} bytes  ({pct:5.1f}%)')

# Estimate: if we could get only IDs
id_val = event.get('id', event.get('_id', ''))
id_size = len(json.dumps(id_val))
print()
print(f'  ID-only payload per event: ~{id_size} bytes')
print(f'  Full event payload:        ~{total} bytes')
print(f'  Potential savings with field selection: {((total - id_size)/total)*100:.1f}%')
" 2>/dev/null <<< "$EVENT_JSON" || echo "  (analysis failed)"

###############################################################################
# 11. SPEED TEST: TIMING COMPARISON
###############################################################################
header "11. TRANSFER SPEED COMPARISON"
echo "  Timing actual requests with limit=1000..."
echo ""

speed_test() {
  local label="$1"; shift
  local total_time
  total_time=$(curl -s -o /dev/null -w '%{time_total}' \
    -H "X-API-Key: $API_KEY" \
    --max-time 30 \
    "$@" 2>/dev/null || echo "99")
  local size_dl
  size_dl=$(curl -s -o /dev/null -w '%{size_download}' \
    -H "X-API-Key: $API_KEY" \
    --max-time 30 \
    "$@" 2>/dev/null || echo "0")
  printf "  %-45s  %8s bytes  %6s sec\n" "$label" "$size_dl" "$total_time"
}

speed_test "No compression" \
  -H "Accept-Encoding: identity" "$API/events?limit=1000"
speed_test "Gzip compression" \
  --compressed "$API/events?limit=1000"
speed_test "Gzip (raw wire bytes)" \
  -H "Accept-Encoding: gzip" --raw "$API/events?limit=1000"

# Test different page sizes for throughput
echo ""
echo "  Events-per-second at different page sizes (with gzip):"
for PSIZE in 100 500 1000 5000 10000; do
  TIMING=$(curl -s -o /dev/null -w '%{time_total}' \
    -H "X-API-Key: $API_KEY" \
    --compressed \
    "$API/events?limit=$PSIZE" 2>/dev/null || echo "99")
  EPS=$(python3 -c "
t = float('$TIMING')
print(f'{$PSIZE/t:.0f}') if t > 0 else print('N/A')
" 2>/dev/null || echo "N/A")
  printf "  limit=%-6s  time=%-8s  events/sec=%-8s\n" "$PSIZE" "${TIMING}s" "$EPS"
done

###############################################################################
# 12. SUMMARY & RECOMMENDATIONS
###############################################################################
header "12. SUMMARY & RECOMMENDATIONS"

echo ""
echo "  All probe results sorted by response size (smallest first):"
echo "  ----------------------------------------------------------------"
sort -t'|' -k1 -n "$RESULTS_FILE" | while IFS='|' read -r size code label; do
  # Only show HTTP 200 results
  if [ "$code" = "200" ]; then
    printf "  %10s bytes  HTTP %-4s  %s\n" "$size" "$code" "$label"
  fi
done

echo ""
echo "  Non-200 responses (may indicate unsupported features):"
echo "  ----------------------------------------------------------------"
sort -t'|' -k1 -n "$RESULTS_FILE" | while IFS='|' read -r size code label; do
  if [ "$code" != "200" ]; then
    printf "  %10s bytes  HTTP %-4s  %s\n" "$size" "$code" "$label"
  fi
done

echo ""
echo "  Smallest 200-OK result:"
SMALLEST=$(sort -t'|' -k1 -n "$RESULTS_FILE" | grep '|200|' | head -1)
if [ -n "$SMALLEST" ]; then
  S_SIZE=$(echo "$SMALLEST" | cut -d'|' -f1)
  S_LABEL=$(echo "$SMALLEST" | cut -d'|' -f3)
  echo "    $S_LABEL => $S_SIZE bytes"
else
  echo "    (no 200 results recorded)"
fi

echo ""
echo "  KEY RECOMMENDATIONS FOR 3M EVENTS:"
echo "  1. Use the largest working page size (test 5000-10000)"
echo "  2. Enable gzip (Accept-Encoding: gzip) if server supports it"
echo "  3. If field selection works, request only 'id' field"
echo "  4. Use concurrent requests with cursor-based pagination"
echo "  5. If streaming endpoint exists, prefer it over pagination"
echo "  6. Look for bulk/export endpoints that bypass pagination"
echo ""

# Cleanup
rm -rf "$TMPDIR_PREFIX"

echo "============================================================"
echo "  Test complete!"
echo "============================================================"
