#!/bin/bash
###############################################################################
# test-cursors.sh
#
# Comprehensive cursor/pagination exploitation analysis for DataSync API.
# Probes cursor format, undocumented query params, limit maximums, parallel
# cursor strategies, and cursor lifecycle behavior.
#
# Usage:
#   API_KEY=your_key_here bash test-cursors.sh
#   # or
#   export API_KEY=your_key_here
#   bash test-cursors.sh
#
# Requires: curl, python3 (for JSON parsing + base64 decoding), jq (optional)
###############################################################################

set -euo pipefail

BASE="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API="$BASE/api/v1"

if [ -z "${API_KEY:-}" ]; then
  echo "ERROR: API_KEY environment variable is required."
  echo "Usage: API_KEY=your_key bash test-cursors.sh"
  exit 1
fi

HEADER="X-API-Key: $API_KEY"
RESULTS_DIR="/tmp/cursor-test-results-$$"
mkdir -p "$RESULTS_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

log_section() {
  echo ""
  echo -e "${BOLD}${CYAN}============================================================${NC}"
  echo -e "${BOLD}${CYAN}  $1${NC}"
  echo -e "${BOLD}${CYAN}============================================================${NC}"
  echo ""
}

log_test() {
  echo -e "${YELLOW}[TEST]${NC} $1"
}

log_result() {
  echo -e "${GREEN}[RESULT]${NC} $1"
}

log_found() {
  echo -e "${GREEN}${BOLD}[FOUND]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

log_info() {
  echo -e "${CYAN}[INFO]${NC} $1"
}

###############################################################################
# SECTION 1: Fetch first page, capture cursor, inspect response fully
###############################################################################
log_section "1. INITIAL REQUEST - CAPTURE CURSOR & FULL RESPONSE"

log_test "Fetching first page with limit=10 (small, fast)"
FIRST_RESPONSE=$(curl -s -D "$RESULTS_DIR/first_headers.txt" \
  -H "$HEADER" \
  "$API/events?limit=10" 2>&1)

echo "$FIRST_RESPONSE" > "$RESULTS_DIR/first_response.json"

# Show all response headers (critical for hints)
log_info "Response Headers:"
cat "$RESULTS_DIR/first_headers.txt"
echo ""

# Extract cursor value
CURSOR=$(echo "$FIRST_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('nextCursor', d.get('next_cursor', d.get('cursor', ''))))
except:
    print('')
" 2>/dev/null)

HAS_MORE=$(echo "$FIRST_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('hasMore', d.get('has_more', '?')))
except:
    print('?')
" 2>/dev/null)

DATA_COUNT=$(echo "$FIRST_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(len(d.get('data', [])))
except:
    print('?')
" 2>/dev/null)

TOP_LEVEL_KEYS=$(echo "$FIRST_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(json.dumps(list(d.keys())))
except:
    print('?')
" 2>/dev/null)

log_result "Data count: $DATA_COUNT | hasMore: $HAS_MORE"
log_result "Top-level keys: $TOP_LEVEL_KEYS"
log_result "Cursor value: $CURSOR"

# Show first event structure
log_info "First event structure:"
echo "$FIRST_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    if d.get('data') and len(d['data']) > 0:
        print(json.dumps(d['data'][0], indent=2))
    else:
        print('No data in response')
except Exception as e:
    print(f'Parse error: {e}')
" 2>/dev/null

# Check for extra metadata fields in response (total count, etc.)
log_info "All non-data fields in response:"
echo "$FIRST_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    meta = {k: v for k, v in d.items() if k != 'data'}
    print(json.dumps(meta, indent=2))
except Exception as e:
    print(f'Parse error: {e}')
" 2>/dev/null

###############################################################################
# SECTION 2: CURSOR FORMAT ANALYSIS - Decode & Reverse Engineer
###############################################################################
log_section "2. CURSOR FORMAT ANALYSIS"

if [ -n "$CURSOR" ] && [ "$CURSOR" != "" ]; then
  log_info "Raw cursor: $CURSOR"
  log_info "Cursor length: ${#CURSOR} characters"

  # 2a. Try base64 decode
  log_test "Base64 decode attempt:"
  B64_DECODED=$(echo "$CURSOR" | python3 -c "
import sys, base64, json
cursor = sys.stdin.read().strip()
# Try standard base64
for variant in [cursor, cursor + '=', cursor + '==']:
    try:
        decoded = base64.b64decode(variant).decode('utf-8', errors='replace')
        print(f'  Standard base64: {decoded}')
        # Check if it's JSON
        try:
            parsed = json.loads(decoded)
            print(f'  Parsed JSON: {json.dumps(parsed, indent=4)}')
        except:
            pass
        break
    except:
        pass

# Try URL-safe base64
for variant in [cursor, cursor + '=', cursor + '==']:
    try:
        decoded = base64.urlsafe_b64decode(variant).decode('utf-8', errors='replace')
        print(f'  URL-safe base64: {decoded}')
        try:
            parsed = json.loads(decoded)
            print(f'  Parsed JSON: {json.dumps(parsed, indent=4)}')
        except:
            pass
        break
    except:
        pass
" 2>/dev/null || echo "  (decode failed)")

  # 2b. Check if it looks like a JWT
  log_test "JWT analysis:"
  echo "$CURSOR" | python3 -c "
import sys
cursor = sys.stdin.read().strip()
parts = cursor.split('.')
if len(parts) == 3:
    print('  LOOKS LIKE JWT! (3 dot-separated segments)')
    import base64, json
    for i, part in enumerate(parts):
        padded = part + '=' * (4 - len(part) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded).decode('utf-8', errors='replace')
            label = ['Header', 'Payload', 'Signature'][i]
            print(f'  {label}: {decoded}')
            try:
                parsed = json.loads(decoded)
                print(f'  {label} (parsed): {json.dumps(parsed, indent=4)}')
            except:
                pass
        except:
            pass
elif len(parts) == 2:
    print('  Two dot-separated segments (may be partial JWT or version.data)')
else:
    print(f'  Not JWT format ({len(parts)} segment(s))')
" 2>/dev/null

  # 2c. Check if it's a numeric/offset value
  log_test "Numeric/offset analysis:"
  echo "$CURSOR" | python3 -c "
import sys
cursor = sys.stdin.read().strip()
# Pure numeric?
try:
    val = int(cursor)
    print(f'  Cursor IS a pure integer: {val}')
    print(f'  If this is an offset, we can forge cursors by computing offsets!')
    print(f'  E.g., worker1=0, worker2={val*50000}, worker3={val*100000}')
except ValueError:
    print(f'  Not a pure integer')

# Hex?
try:
    val = int(cursor, 16)
    print(f'  Cursor IS valid hex: decimal={val}')
except ValueError:
    print(f'  Not valid hex')

# UUID format?
import re
if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', cursor, re.I):
    print(f'  Cursor IS a UUID')
elif re.match(r'^[0-9a-f]{24}$', cursor, re.I):
    print(f'  Cursor looks like a MongoDB ObjectId')
elif re.match(r'^[0-9a-f]{32}$', cursor, re.I):
    print(f'  Cursor looks like a hex-encoded UUID (no dashes)')
else:
    print(f'  Not UUID or ObjectId format')

# URL-encoded?
from urllib.parse import unquote
decoded = unquote(cursor)
if decoded != cursor:
    print(f'  URL decoded: {decoded}')
else:
    print(f'  No URL encoding detected')

# Check character set
charset = set(cursor)
has_upper = any(c.isupper() for c in cursor)
has_lower = any(c.islower() for c in cursor)
has_digits = any(c.isdigit() for c in cursor)
has_special = any(not c.isalnum() for c in cursor)
print(f'  Character analysis: upper={has_upper}, lower={has_lower}, digits={has_digits}, special={has_special}')
print(f'  Unique chars: {sorted(charset)}')
" 2>/dev/null

  # 2d. Fetch second page and compare cursors for pattern detection
  log_test "Fetching second page to compare cursor patterns..."
  SECOND_RESPONSE=$(curl -s -H "$HEADER" "$API/events?limit=10&cursor=$CURSOR" 2>&1)
  CURSOR2=$(echo "$SECOND_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('nextCursor', d.get('next_cursor', d.get('cursor', ''))))
except:
    print('')
" 2>/dev/null)

  log_info "Cursor 1: $CURSOR"
  log_info "Cursor 2: $CURSOR2"

  echo "$CURSOR" | python3 -c "
import sys
c1 = sys.stdin.read().strip()
" 2>/dev/null
  # Compare the two cursors
  python3 -c "
c1 = '''$CURSOR'''
c2 = '''$CURSOR2'''
print(f'Cursor 1 length: {len(c1)}')
print(f'Cursor 2 length: {len(c2)}')
print(f'Same length: {len(c1) == len(c2)}')

# Try to detect incrementing pattern
import base64, json
for label, cursor in [('Cursor1', c1), ('Cursor2', c2)]:
    for variant in [cursor, cursor + '=', cursor + '==']:
        try:
            decoded = base64.b64decode(variant).decode('utf-8')
            try:
                parsed = json.loads(decoded)
                print(f'{label} decoded JSON: {parsed}')
            except:
                print(f'{label} decoded text: {decoded}')
            break
        except:
            pass
    for variant in [cursor, cursor + '=', cursor + '==']:
        try:
            decoded = base64.urlsafe_b64decode(variant).decode('utf-8')
            try:
                parsed = json.loads(decoded)
                print(f'{label} url-safe decoded JSON: {parsed}')
            except:
                pass
            break
        except:
            pass
" 2>/dev/null || true

else
  log_error "No cursor found in initial response!"
  log_info "Response may not use cursor pagination, or format is different."
fi

###############################################################################
# SECTION 3: CURSOR FORGING TESTS
###############################################################################
log_section "3. CURSOR FORGING TESTS"

log_info "Attempting to forge cursors to enable parallel workers..."

# Try common cursor formats: base64-encoded JSON with offset
FORGED_CURSORS=(
  # base64-encoded JSON offsets
  "eyJvZmZzZXQiOjEwMDB9"           # {"offset":1000}
  "eyJvZmZzZXQiOjUwMDAwMH0="       # {"offset":500000}
  "eyJvZmZzZXQiOjEwMDAwMDB9"       # {"offset":1000000}
  "eyJvZmZzZXQiOjIwMDAwMDB9"       # {"offset":2000000}
  "eyJza2lwIjoxMDAwfQ=="            # {"skip":1000}
  "eyJwYWdlIjoxMH0="               # {"page":10}
  "eyJpZCI6MTAwMH0="               # {"id":1000}
  "eyJsYXN0SWQiOjEwMDB9"           # {"lastId":1000}
  "eyJzdGFydCI6MTAwMH0="           # {"start":1000}
  # Plain numbers
  "1000"
  "500000"
  "1000000"
  # Hex offsets
  "03e8"     # 1000 in hex
  "0f4240"   # 1000000 in hex
)

FORGED_LABELS=(
  '{"offset":1000}'
  '{"offset":500000}'
  '{"offset":1000000}'
  '{"offset":2000000}'
  '{"skip":1000}'
  '{"page":10}'
  '{"id":1000}'
  '{"lastId":1000}'
  '{"start":1000}'
  'plain:1000'
  'plain:500000'
  'plain:1000000'
  'hex:1000'
  'hex:1000000'
)

for i in "${!FORGED_CURSORS[@]}"; do
  FORGED="${FORGED_CURSORS[$i]}"
  LABEL="${FORGED_LABELS[$i]}"
  log_test "Forged cursor ($LABEL) => $FORGED"

  RESP=$(curl -s -o "$RESULTS_DIR/forged_$i.json" -w "%{http_code}" \
    -H "$HEADER" \
    "$API/events?limit=2&cursor=$FORGED" 2>&1)

  if [ "$RESP" = "200" ]; then
    COUNT=$(python3 -c "
import json
try:
    with open('$RESULTS_DIR/forged_$i.json') as f:
        d = json.load(f)
    data = d.get('data', [])
    print(f'HTTP 200 - {len(data)} events returned')
    if data:
        first_id = data[0].get('id', data[0].get('eventId', 'unknown'))
        print(f'  First event ID: {first_id}')
except Exception as e:
    print(f'Parse error: {e}')
" 2>/dev/null)
    log_found "FORGED CURSOR ACCEPTED! $COUNT"
  else
    ERROR_MSG=$(python3 -c "
import json
try:
    with open('$RESULTS_DIR/forged_$i.json') as f:
        content = f.read()
    try:
        d = json.loads(content)
        print(d.get('message', d.get('error', content[:100])))
    except:
        print(content[:100])
except:
    print('(no body)')
" 2>/dev/null)
    log_result "HTTP $RESP - $ERROR_MSG"
  fi
done

###############################################################################
# SECTION 4: UNDOCUMENTED QUERY PARAMETERS
###############################################################################
log_section "4. UNDOCUMENTED QUERY PARAMETER PROBING"

log_info "Testing every plausible hidden query parameter..."
log_info "Looking for: alternate pagination, range queries, sorting, partitioning"

# Associative array: param=value => description
declare -a PARAMS
declare -a PARAM_DESCS

# Sorting / ordering
PARAMS+=("sort=asc")             ; PARAM_DESCS+=("Sort ascending")
PARAMS+=("sort=desc")            ; PARAM_DESCS+=("Sort descending")
PARAMS+=("order=asc")            ; PARAM_DESCS+=("Order ascending")
PARAMS+=("order=desc")           ; PARAM_DESCS+=("Order descending")
PARAMS+=("sortBy=id")            ; PARAM_DESCS+=("Sort by id")
PARAMS+=("sortBy=createdAt")     ; PARAM_DESCS+=("Sort by createdAt")
PARAMS+=("sort_by=id&sort_order=desc")  ; PARAM_DESCS+=("Sort by id desc (snake_case)")
PARAMS+=("orderBy=id&direction=desc")   ; PARAM_DESCS+=("Order by id desc")

# Direct offset / page-based pagination
PARAMS+=("offset=0")             ; PARAM_DESCS+=("Offset=0 (offset-based pagination)")
PARAMS+=("offset=1000")          ; PARAM_DESCS+=("Offset=1000")
PARAMS+=("offset=500000")        ; PARAM_DESCS+=("Offset=500000 (skip ahead)")
PARAMS+=("offset=1000000")       ; PARAM_DESCS+=("Offset=1000000 (big skip)")
PARAMS+=("page=1")               ; PARAM_DESCS+=("Page=1 (page-based)")
PARAMS+=("page=2")               ; PARAM_DESCS+=("Page=2")
PARAMS+=("page=1000")            ; PARAM_DESCS+=("Page=1000 (deep page)")
PARAMS+=("skip=1000")            ; PARAM_DESCS+=("Skip=1000")
PARAMS+=("start=1000")           ; PARAM_DESCS+=("Start=1000")
PARAMS+=("from=0")               ; PARAM_DESCS+=("From=0")
PARAMS+=("from=1000000")         ; PARAM_DESCS+=("From=1000000")

# ID-based range queries
PARAMS+=("after=1000")           ; PARAM_DESCS+=("After ID 1000")
PARAMS+=("before=2000000")       ; PARAM_DESCS+=("Before ID 2000000")
PARAMS+=("since=1000")           ; PARAM_DESCS+=("Since ID 1000")
PARAMS+=("until=2000000")        ; PARAM_DESCS+=("Until ID 2000000")
PARAMS+=("id_gt=1000")           ; PARAM_DESCS+=("ID greater than 1000")
PARAMS+=("id_gte=1000")          ; PARAM_DESCS+=("ID greater than or equal 1000")
PARAMS+=("id_lt=2000000")        ; PARAM_DESCS+=("ID less than 2000000")
PARAMS+=("id_lte=2000000")       ; PARAM_DESCS+=("ID less than or equal 2000000")
PARAMS+=("min_id=500000")        ; PARAM_DESCS+=("Min ID 500000")
PARAMS+=("max_id=1000000")       ; PARAM_DESCS+=("Max ID 1000000")
PARAMS+=("startId=500000")       ; PARAM_DESCS+=("Start ID 500000")
PARAMS+=("endId=1000000")        ; PARAM_DESCS+=("End ID 1000000")

# Date/time range queries
PARAMS+=("startDate=2024-01-01") ; PARAM_DESCS+=("Start date 2024-01-01")
PARAMS+=("endDate=2024-06-01")   ; PARAM_DESCS+=("End date 2024-06-01")
PARAMS+=("startDate=2024-01-01&endDate=2024-06-01") ; PARAM_DESCS+=("Date range Jan-Jun 2024")
PARAMS+=("from_date=2024-01-01") ; PARAM_DESCS+=("From date 2024-01-01")
PARAMS+=("to_date=2024-12-31")   ; PARAM_DESCS+=("To date 2024-12-31")
PARAMS+=("created_after=2024-01-01T00:00:00Z")  ; PARAM_DESCS+=("Created after ISO timestamp")
PARAMS+=("created_before=2024-12-31T23:59:59Z") ; PARAM_DESCS+=("Created before ISO timestamp")
PARAMS+=("since=2024-01-01T00:00:00Z")          ; PARAM_DESCS+=("Since ISO timestamp")
PARAMS+=("start_time=2024-01-01T00:00:00Z")     ; PARAM_DESCS+=("Start time ISO")
PARAMS+=("end_time=2024-06-01T00:00:00Z")       ; PARAM_DESCS+=("End time ISO")

# Partitioning / segmenting (for parallel download)
PARAMS+=("partition=0&partitions=6")   ; PARAM_DESCS+=("Partition 0 of 6")
PARAMS+=("partition=1&partitions=6")   ; PARAM_DESCS+=("Partition 1 of 6")
PARAMS+=("segment=0&segments=6")       ; PARAM_DESCS+=("Segment 0 of 6")
PARAMS+=("segment=1&segments=6")       ; PARAM_DESCS+=("Segment 1 of 6")
PARAMS+=("chunk=0&chunks=6")           ; PARAM_DESCS+=("Chunk 0 of 6")
PARAMS+=("chunk=1&chunks=6")           ; PARAM_DESCS+=("Chunk 1 of 6")
PARAMS+=("shard=0&shards=6")           ; PARAM_DESCS+=("Shard 0 of 6")
PARAMS+=("range=0-500000")             ; PARAM_DESCS+=("Range 0-500000")
PARAMS+=("range=500000-1000000")       ; PARAM_DESCS+=("Range 500K-1M")
PARAMS+=("batch=1")                    ; PARAM_DESCS+=("Batch 1")
PARAMS+=("batch=2")                    ; PARAM_DESCS+=("Batch 2")
PARAMS+=("worker=0&workers=6")         ; PARAM_DESCS+=("Worker 0 of 6")

# Filter / search params
PARAMS+=("type=click")                ; PARAM_DESCS+=("Filter type=click")
PARAMS+=("category=page_view")        ; PARAM_DESCS+=("Filter category=page_view")
PARAMS+=("source=web")                ; PARAM_DESCS+=("Filter source=web")
PARAMS+=("channel=web")               ; PARAM_DESCS+=("Filter channel=web")
PARAMS+=("fields=id")                 ; PARAM_DESCS+=("Fields=id only (sparse)")
PARAMS+=("select=id")                 ; PARAM_DESCS+=("Select=id only")
PARAMS+=("ids_only=true")             ; PARAM_DESCS+=("IDs only flag")
PARAMS+=("format=ids")                ; PARAM_DESCS+=("Format=ids")
PARAMS+=("format=csv")                ; PARAM_DESCS+=("Format=csv")
PARAMS+=("format=ndjson")             ; PARAM_DESCS+=("Format=ndjson")
PARAMS+=("compact=true")              ; PARAM_DESCS+=("Compact mode")
PARAMS+=("slim=true")                 ; PARAM_DESCS+=("Slim mode")

# First, get a "baseline" response to compare against
log_test "Fetching baseline: limit=5, no extra params"
BASELINE=$(curl -s -H "$HEADER" "$API/events?limit=5")
BASELINE_COUNT=$(echo "$BASELINE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(len(d.get('data', [])))
except:
    print(0)
" 2>/dev/null)
BASELINE_FIRST_ID=$(echo "$BASELINE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    evt = d['data'][0]
    print(evt.get('id', evt.get('eventId', evt.get('_id', 'unknown'))))
except:
    print('unknown')
" 2>/dev/null)
log_info "Baseline: $BASELINE_COUNT events, first ID: $BASELINE_FIRST_ID"

echo ""
log_info "Testing ${#PARAMS[@]} parameter combinations..."
echo ""

INTERESTING_PARAMS=()

for i in "${!PARAMS[@]}"; do
  PARAM="${PARAMS[$i]}"
  DESC="${PARAM_DESCS[$i]}"

  # Use limit=5 to be fast; include the extra params
  RESP_CODE=$(curl -s -o "$RESULTS_DIR/param_$i.json" -w "%{http_code}" \
    -H "$HEADER" \
    "$API/events?limit=5&$PARAM" 2>&1)

  ANALYSIS=$(python3 -c "
import json, sys
try:
    with open('$RESULTS_DIR/param_$i.json') as f:
        d = json.load(f)
    data = d.get('data', [])
    count = len(data)
    has_more = d.get('hasMore', '?')
    extra_keys = [k for k in d.keys() if k not in ('data', 'hasMore', 'nextCursor')]
    first_id = 'none'
    if data:
        evt = data[0]
        first_id = evt.get('id', evt.get('eventId', evt.get('_id', 'unknown')))

    # Detect if this param changed behavior
    interesting = False
    if str(first_id) != '$BASELINE_FIRST_ID' and count > 0:
        interesting = True
    if extra_keys:
        interesting = True
    if count != $BASELINE_COUNT and count > 0:
        interesting = True

    prefix = 'INTERESTING' if interesting else 'normal'
    print(f'{prefix}|{count} events|firstId={first_id}|hasMore={has_more}|extraKeys={extra_keys}')
except Exception as e:
    # Check if non-JSON (maybe CSV, etc.)
    try:
        with open('$RESULTS_DIR/param_$i.json') as f:
            raw = f.read()[:200]
        print(f'NON-JSON|{raw}')
    except:
        print(f'ERROR|{e}')
" 2>/dev/null)

  STATUS_MARKER=""
  if [ "$RESP_CODE" != "200" ]; then
    STATUS_MARKER=" [HTTP $RESP_CODE]"
  fi

  if echo "$ANALYSIS" | grep -q "^INTERESTING"; then
    log_found "$DESC ($PARAM)$STATUS_MARKER => $ANALYSIS"
    INTERESTING_PARAMS+=("$PARAM")
  elif echo "$ANALYSIS" | grep -q "^NON-JSON"; then
    log_found "$DESC ($PARAM)$STATUS_MARKER => NON-JSON response: $ANALYSIS"
    INTERESTING_PARAMS+=("$PARAM")
  elif [ "$RESP_CODE" != "200" ] && [ "$RESP_CODE" != "400" ]; then
    log_result "$DESC ($PARAM) => HTTP $RESP_CODE (unexpected status)"
  else
    # Normal - print compact
    echo "  . $DESC ($PARAM) => HTTP $RESP_CODE | $ANALYSIS"
  fi
done

echo ""
if [ ${#INTERESTING_PARAMS[@]} -gt 0 ]; then
  log_section "4b. INTERESTING PARAMETERS - DEEP DIVE"
  for PARAM in "${INTERESTING_PARAMS[@]}"; do
    log_test "Deep dive: $PARAM (limit=50)"
    curl -s -H "$HEADER" "$API/events?limit=50&$PARAM" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    data = d.get('data', [])
    print(f'  Events returned: {len(data)}')
    if data:
        ids = [str(e.get('id', e.get('eventId', '?'))) for e in data[:5]]
        print(f'  First 5 IDs: {ids}')
        ids_end = [str(e.get('id', e.get('eventId', '?'))) for e in data[-3:]]
        print(f'  Last 3 IDs: {ids_end}')
    print(f'  hasMore: {d.get(\"hasMore\", \"?\")}')
    extra = {k: v for k, v in d.items() if k not in ('data', 'hasMore', 'nextCursor')}
    if extra:
        print(f'  Extra metadata: {json.dumps(extra)}')
except Exception as e:
    print(f'  Error: {e}')
" 2>/dev/null
    echo ""
  done
fi

###############################################################################
# SECTION 5: LIMIT PARAMETER - FIND MAXIMUM
###############################################################################
log_section "5. LIMIT PARAMETER - MAXIMUM PAGE SIZE"

log_info "Testing increasingly large limit values..."
log_info "If limit=50000+ works, we only need ~60 requests for 3M events."
echo ""

LIMITS=(50 100 200 500 1000 2000 5000 10000 25000 50000 100000)

for LIM in "${LIMITS[@]}"; do
  log_test "limit=$LIM"
  START_TIME=$(python3 -c "import time; print(time.time())")

  RESP_CODE=$(curl -s -o "$RESULTS_DIR/limit_$LIM.json" -w "%{http_code}" \
    -D "$RESULTS_DIR/limit_headers_$LIM.txt" \
    -H "$HEADER" \
    --max-time 30 \
    "$API/events?limit=$LIM" 2>&1)

  END_TIME=$(python3 -c "import time; print(time.time())")
  ELAPSED=$(python3 -c "print(f'{$END_TIME - $START_TIME:.2f}')")

  if [ "$RESP_CODE" = "200" ]; then
    ANALYSIS=$(python3 -c "
import json, os
try:
    with open('$RESULTS_DIR/limit_$LIM.json') as f:
        d = json.load(f)
    count = len(d.get('data', []))
    size_bytes = os.path.getsize('$RESULTS_DIR/limit_$LIM.json')
    size_mb = size_bytes / 1024 / 1024
    print(f'Got {count} events | Response: {size_mb:.2f} MB | Effective: {count == $LIM}')
except Exception as e:
    print(f'Parse error: {e}')
" 2>/dev/null)
    log_result "limit=$LIM => HTTP 200 | ${ELAPSED}s | $ANALYSIS"

    # Show rate limit headers
    RL_HEADERS=$(grep -i -E "ratelimit|x-rate|retry-after" "$RESULTS_DIR/limit_headers_$LIM.txt" 2>/dev/null | tr '\n' ' ' || echo "(none)")
    if [ -n "$RL_HEADERS" ] && [ "$RL_HEADERS" != "(none)" ]; then
      log_info "  Rate limit headers: $RL_HEADERS"
    fi
  else
    ERROR_BODY=$(python3 -c "
try:
    with open('$RESULTS_DIR/limit_$LIM.json') as f:
        print(f.read()[:200])
except:
    print('(no body)')
" 2>/dev/null)
    log_result "limit=$LIM => HTTP $RESP_CODE | ${ELAPSED}s | $ERROR_BODY"
  fi
done

###############################################################################
# SECTION 6: PARALLEL CURSOR CHAINS
###############################################################################
log_section "6. PARALLEL CURSOR CHAIN TESTS"

log_info "Can we open multiple independent cursor chains simultaneously?"
log_info "Testing if multiple first-page requests give independent cursors..."
echo ""

log_test "Requesting 3 independent first pages simultaneously..."

# Fire 3 parallel requests
curl -s -H "$HEADER" "$API/events?limit=5" > "$RESULTS_DIR/chain_a.json" &
PID_A=$!
curl -s -H "$HEADER" "$API/events?limit=5" > "$RESULTS_DIR/chain_b.json" &
PID_B=$!
curl -s -H "$HEADER" "$API/events?limit=5" > "$RESULTS_DIR/chain_c.json" &
PID_C=$!

wait $PID_A $PID_B $PID_C 2>/dev/null

python3 -c "
import json
chains = {}
for label in ['a', 'b', 'c']:
    try:
        with open(f'$RESULTS_DIR/chain_{label}.json') as f:
            d = json.load(f)
        cursor = d.get('nextCursor', d.get('next_cursor', ''))
        first_id = 'none'
        data = d.get('data', [])
        if data:
            first_id = data[0].get('id', data[0].get('eventId', 'unknown'))
        chains[label] = {'cursor': cursor, 'first_id': str(first_id), 'count': len(data)}
        print(f'  Chain {label}: firstId={first_id}, cursor={cursor[:40]}...' if len(cursor) > 40 else f'  Chain {label}: firstId={first_id}, cursor={cursor}')
    except Exception as e:
        print(f'  Chain {label}: ERROR - {e}')

# Check if cursors are identical or different
cursors = [c.get('cursor', '') for c in chains.values()]
if len(set(cursors)) == 1:
    print('  => All cursors IDENTICAL. Cannot start parallel chains from same endpoint.')
    print('  => Need different starting parameters (offset, page, sort) to parallelize.')
elif len(set(cursors)) > 1:
    print('  => Cursors are DIFFERENT! Each request may get an independent cursor chain.')
    print('  => This could enable parallel traversal!')
" 2>/dev/null

# Test: Can we use two cursors from the same chain simultaneously?
if [ -n "$CURSOR" ]; then
  log_test "Advancing cursor A, then using both cursors (A+B) in parallel..."

  CURSOR_A="$CURSOR"  # From our original first page

  # Get a second cursor by advancing chain A
  RESP_A2=$(curl -s -H "$HEADER" "$API/events?limit=5&cursor=$CURSOR_A")
  CURSOR_A2=$(echo "$RESP_A2" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('nextCursor', d.get('next_cursor', '')))
except:
    print('')
" 2>/dev/null)

  # Now use both cursors simultaneously
  curl -s -H "$HEADER" "$API/events?limit=5&cursor=$CURSOR_A" > "$RESULTS_DIR/parallel_old.json" &
  PID_OLD=$!
  curl -s -H "$HEADER" "$API/events?limit=5&cursor=$CURSOR_A2" > "$RESULTS_DIR/parallel_new.json" &
  PID_NEW=$!
  wait $PID_OLD $PID_NEW 2>/dev/null

  python3 -c "
import json
for label, fname in [('reused_cursor', 'parallel_old.json'), ('new_cursor', 'parallel_new.json')]:
    try:
        with open(f'$RESULTS_DIR/{fname}') as f:
            d = json.load(f)
        data = d.get('data', [])
        if data:
            ids = [str(e.get('id', e.get('eventId', '?'))) for e in data[:3]]
            print(f'  {label}: {len(data)} events, IDs: {ids}')
        else:
            err = d.get('error', d.get('message', 'empty data'))
            print(f'  {label}: No data - {err}')
    except Exception as e:
        print(f'  {label}: ERROR - {e}')
" 2>/dev/null
fi

###############################################################################
# SECTION 7: CURSOR LIFECYCLE / EXPIRATION TESTS
###############################################################################
log_section "7. CURSOR LIFECYCLE ANALYSIS"

log_info "Testing cursor expiration behavior (CURSOR_REFRESH_THRESHOLD=60s hint)"
log_info "Note: This test takes time. We will:"
log_info "  1. Get a fresh cursor"
log_info "  2. Use it immediately (should work)"
log_info "  3. Wait 30s and try again"
log_info "  4. Wait 30s more (total 60s) and try again"
echo ""

# Get a fresh cursor
log_test "Getting fresh cursor..."
FRESH_RESP=$(curl -s -H "$HEADER" "$API/events?limit=5")
LIFECYCLE_CURSOR=$(echo "$FRESH_RESP" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('nextCursor', d.get('next_cursor', '')))
except:
    print('')
" 2>/dev/null)

if [ -n "$LIFECYCLE_CURSOR" ]; then
  # Immediate use
  log_test "Immediate use (T+0s)..."
  RESP_0=$(curl -s -o "$RESULTS_DIR/lifecycle_0.json" -w "%{http_code}" \
    -H "$HEADER" \
    "$API/events?limit=5&cursor=$LIFECYCLE_CURSOR")
  log_result "T+0s: HTTP $RESP_0"

  # Pre-fetch the NEXT cursor immediately (pipeline strategy)
  NEXT_CURSOR_PREFETCHED=$(curl -s -H "$HEADER" \
    "$API/events?limit=5&cursor=$LIFECYCLE_CURSOR" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('nextCursor', d.get('next_cursor', '')))
except:
    print('')
" 2>/dev/null)
  log_info "Pre-fetched next cursor for pipeline test: ${NEXT_CURSOR_PREFETCHED:0:40}..."

  # Wait 30 seconds
  log_test "Waiting 30 seconds..."
  sleep 30

  log_test "Reuse after 30s (T+30s) - original cursor..."
  RESP_30=$(curl -s -o "$RESULTS_DIR/lifecycle_30.json" -w "%{http_code}" \
    -H "$HEADER" \
    "$API/events?limit=5&cursor=$LIFECYCLE_CURSOR")
  BODY_30=$(python3 -c "
import json
try:
    with open('$RESULTS_DIR/lifecycle_30.json') as f:
        d = json.load(f)
    count = len(d.get('data', []))
    err = d.get('error', d.get('message', ''))
    print(f'{count} events, error={err}' if err else f'{count} events')
except Exception as e:
    print(str(e))
" 2>/dev/null)
  log_result "T+30s: HTTP $RESP_30 | $BODY_30"

  log_test "Reuse pre-fetched cursor after 30s..."
  RESP_30_PF=$(curl -s -o "$RESULTS_DIR/lifecycle_30pf.json" -w "%{http_code}" \
    -H "$HEADER" \
    "$API/events?limit=5&cursor=$NEXT_CURSOR_PREFETCHED")
  log_result "T+30s (pre-fetched): HTTP $RESP_30_PF"

  # Wait another 35 seconds (total ~65s, beyond 60s threshold)
  log_test "Waiting 35 more seconds (total ~65s from original fetch)..."
  sleep 35

  log_test "Reuse after ~65s (T+65s) - original cursor..."
  RESP_65=$(curl -s -o "$RESULTS_DIR/lifecycle_65.json" -w "%{http_code}" \
    -H "$HEADER" \
    "$API/events?limit=5&cursor=$LIFECYCLE_CURSOR")
  BODY_65=$(python3 -c "
import json
try:
    with open('$RESULTS_DIR/lifecycle_65.json') as f:
        d = json.load(f)
    count = len(d.get('data', []))
    err = d.get('error', d.get('message', ''))
    print(f'{count} events, error={err}' if err else f'{count} events')
except Exception as e:
    print(str(e))
" 2>/dev/null)
  log_result "T+65s: HTTP $RESP_65 | $BODY_65"

  log_test "Reuse pre-fetched cursor after ~65s..."
  RESP_65_PF=$(curl -s -o "$RESULTS_DIR/lifecycle_65pf.json" -w "%{http_code}" \
    -H "$HEADER" \
    "$API/events?limit=5&cursor=$NEXT_CURSOR_PREFETCHED")
  BODY_65_PF=$(python3 -c "
import json
try:
    with open('$RESULTS_DIR/lifecycle_65pf.json') as f:
        d = json.load(f)
    count = len(d.get('data', []))
    err = d.get('error', d.get('message', ''))
    print(f'{count} events, error={err}' if err else f'{count} events')
except Exception as e:
    print(str(e))
" 2>/dev/null)
  log_result "T+65s (pre-fetched): HTTP $RESP_65_PF | $BODY_65_PF"

  # Can we "refresh" an expired cursor?
  log_test "Can we re-fetch a fresh cursor and continue from same position?"
  FRESH_RESP2=$(curl -s -H "$HEADER" "$API/events?limit=5")
  FRESH_CURSOR2=$(echo "$FRESH_RESP2" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('nextCursor', d.get('next_cursor', '')))
except:
    print('')
" 2>/dev/null)
  log_result "New cursor from fresh request: ${FRESH_CURSOR2:0:40}..."
  if [ "$FRESH_CURSOR2" = "$LIFECYCLE_CURSOR" ]; then
    log_found "SAME cursor returned! Server remembers position per API key."
  else
    log_result "DIFFERENT cursor. Each fresh request starts a new chain."
  fi
else
  log_error "No cursor available for lifecycle testing."
fi

###############################################################################
# SECTION 8: RESPONSE HEADER DEEP INSPECTION
###############################################################################
log_section "8. RESPONSE HEADER ANALYSIS"

log_info "Checking all headers for hints about bulk endpoints, pagination, etc."

curl -s -D "$RESULTS_DIR/detailed_headers.txt" -o /dev/null \
  -H "$HEADER" \
  "$API/events?limit=10"

echo "All response headers:"
cat "$RESULTS_DIR/detailed_headers.txt"
echo ""

# Look for specific hint headers
log_test "Checking for hint/link headers..."
grep -i -E "link|x-total|x-count|x-page|x-hint|x-bulk|x-export|x-stream|x-fast|x-alternate|x-method" \
  "$RESULTS_DIR/detailed_headers.txt" 2>/dev/null || echo "  (no special hint headers found)"

# Check OPTIONS for allowed methods
log_test "OPTIONS /events - checking allowed methods..."
curl -s -D - -o /dev/null -X OPTIONS -H "$HEADER" "$API/events" 2>&1 | head -20

###############################################################################
# SECTION 9: ADDITIONAL ENDPOINT DISCOVERY
###############################################################################
log_section "9. ADDITIONAL ENDPOINT & FORMAT DISCOVERY"

# Try getting events in different formats via Accept header
log_test "Testing Accept headers for bulk/streaming formats..."
ACCEPT_HEADERS=(
  "text/csv"
  "application/ndjson"
  "application/x-ndjson"
  "text/plain"
  "text/tab-separated-values"
  "application/octet-stream"
  "text/event-stream"
  "application/jsonl"
  "application/json-seq"
)

for ACC in "${ACCEPT_HEADERS[@]}"; do
  RESP_CODE=$(curl -s -o "$RESULTS_DIR/accept_test.txt" -w "%{http_code}" \
    -D "$RESULTS_DIR/accept_headers.txt" \
    -H "$HEADER" \
    -H "Accept: $ACC" \
    "$API/events?limit=5" 2>&1)

  CT=$(grep -i "content-type" "$RESULTS_DIR/accept_headers.txt" 2>/dev/null | head -1 | tr -d '\r')
  PREVIEW=$(head -c 200 "$RESULTS_DIR/accept_test.txt" 2>/dev/null)

  if [ "$RESP_CODE" = "200" ] && ! echo "$CT" | grep -qi "application/json"; then
    log_found "Accept: $ACC => HTTP $RESP_CODE | $CT"
    log_info "  Preview: $PREVIEW"
  else
    echo "  . Accept: $ACC => HTTP $RESP_CODE | $CT"
  fi
done

# Test streaming endpoint patterns
log_test "Testing streaming/export endpoint patterns..."
STREAM_ENDPOINTS=(
  "/events/stream"
  "/events/export"
  "/events/download"
  "/events/bulk"
  "/events/dump"
  "/events/all"
  "/events/ids"
  "/events/list"
  "/events/batch"
  "/events/fetch"
  "/events/data"
  "/events/raw"
  "/events.csv"
  "/events.json"
  "/events.ndjson"
  "/export/events"
  "/stream/events"
  "/download/events"
  "/bulk/events"
  "/data/events"
)

for EP in "${STREAM_ENDPOINTS[@]}"; do
  RESP_CODE=$(curl -s -o "$RESULTS_DIR/ep_test.txt" -w "%{http_code}" \
    -H "$HEADER" \
    --max-time 10 \
    "$API$EP" 2>&1)

  if [ "$RESP_CODE" != "404" ] && [ "$RESP_CODE" != "000" ]; then
    PREVIEW=$(head -c 300 "$RESULTS_DIR/ep_test.txt" 2>/dev/null)
    log_found "Endpoint $EP => HTTP $RESP_CODE"
    log_info "  Preview: $PREVIEW"
  else
    echo "  . $EP => HTTP $RESP_CODE"
  fi
done

###############################################################################
# SECTION 10: SUMMARY & RECOMMENDATIONS
###############################################################################
log_section "10. SUMMARY & RECOMMENDATIONS"

echo "Test results saved to: $RESULTS_DIR/"
echo ""
echo "Key files to review:"
echo "  $RESULTS_DIR/first_response.json    - First API response (full)"
echo "  $RESULTS_DIR/first_headers.txt       - First response headers"
echo "  $RESULTS_DIR/detailed_headers.txt    - Detailed header inspection"
echo ""

python3 -c "
print('''
=== STRATEGY DECISION MATRIX ===

Based on test results, determine your ingestion strategy:

SCENARIO A: Forged cursors work (offset-based cursor)
  -> Split 3M events into N ranges (e.g., 6 workers x 500K each)
  -> Each worker forges its own starting cursor
  -> Parallel ingestion in ~1/6th the time
  -> BEST CASE: If limit=50K works too, only 10 requests per worker = 60 total

SCENARIO B: Bulk/export endpoint found
  -> Single request (or few requests) to download all 3M events
  -> Parse response (CSV/NDJSON/JSON) and bulk insert into DB
  -> BEST CASE: 1-3 requests total

SCENARIO C: Large page sizes work (limit=10000+)
  -> 3M / 10K = 300 requests (manageable in minutes)
  -> Use cursor prefetching: while inserting batch N, fetch batch N+1
  -> Pipeline: fetch -> parse -> insert (overlap network + DB I/O)

SCENARIO D: Undocumented params enable parallelism (offset, partition, date-range)
  -> Split workload across multiple independent cursor chains
  -> Each chain handles a subset (by offset, date range, or partition)

SCENARIO E: No shortcuts found (standard pagination only)
  -> Use maximum discovered limit value
  -> Pipeline cursor consumption with DB insertion
  -> Pre-fetch next cursor immediately to avoid expiration
  -> Rate limit aware: burst requests, sleep only when needed
  -> With limit=1000 and 10 req/s: 3000 requests x 0.1s = 5 min + DB time

=== CURSOR EXPIRATION MITIGATION ===

If cursors expire after 60s:
  1. NEVER buffer more than ~50s of work between cursor fetches
  2. Pre-fetch pattern: request page N+1 cursor while processing page N
  3. If cursor expires, restart from last saved checkpoint (resumability)
  4. Save progress every N pages to DB for crash recovery
  5. Consider multiple concurrent cursor chains if the API allows it

=== RATE LIMIT OPTIMIZATION ===

  1. Use X-API-Key header (not query param) for better limits
  2. Monitor X-RateLimit-Remaining header
  3. Burst when remaining > 10, throttle when remaining < 5
  4. If 429 received, respect Retry-After header exactly
  5. Multiple workers should share rate limit state
''')
" 2>/dev/null

echo ""
echo "=== TEST COMPLETE ==="
echo "Review the output above to determine the optimal ingestion strategy."
echo "Results directory: $RESULTS_DIR"
