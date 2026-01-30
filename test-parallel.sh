#!/bin/bash
# =============================================================================
# Parallel / Concurrency Exploitation Test Script for DataSync API
# =============================================================================
# Systematically discovers the maximum throughput achievable by:
#   1. Concurrency scaling (find max parallel requests before 429s)
#   2. Multi-cursor pipeline (parallel cursor consumption)
#   3. Dual-auth sustained throughput (header vs query param buckets)
#   4. Multi-worker simulation (realistic N-worker ingestion)
#   5. Request pipelining (connection reuse vs new connections)
#   6. Burst pattern testing (burst + wait cycles)
#   7. Mixed strategy (combine all working techniques)
#   8. Summary with projected 3M ingestion times
#
# Usage:
#   API_KEY=your_key_here bash test-parallel.sh
#
# Budget: ~200-300 API calls total
# =============================================================================

set -uo pipefail

BASE="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API_V1="$BASE/api/v1"

if [ -z "${API_KEY:-}" ]; then
  echo "ERROR: API_KEY environment variable is required."
  echo "Usage: API_KEY=your_key bash test-parallel.sh"
  exit 1
fi

TMPDIR_P=$(mktemp -d)
trap 'rm -rf "$TMPDIR_P"' EXIT

TOTAL_CALLS=0
RESULTS_FILE="$TMPDIR_P/results_summary.txt"
: > "$RESULTS_FILE"

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
if [ -t 1 ]; then
  BOLD="\033[1m" GREEN="\033[32m" YELLOW="\033[33m" RED="\033[31m"
  CYAN="\033[36m" MAGENTA="\033[35m" RESET="\033[0m"
else
  BOLD="" GREEN="" YELLOW="" RED="" CYAN="" MAGENTA="" RESET=""
fi

banner()  { echo -e "\n${BOLD}${CYAN}================================================================${RESET}"; echo -e "${BOLD}${CYAN}  $1${RESET}"; echo -e "${BOLD}${CYAN}================================================================${RESET}"; }
section() { echo -e "\n${BOLD}${MAGENTA}--- $1 ---${RESET}"; }
info()    { echo -e "  ${GREEN}[+]${RESET} $1"; }
warn()    { echo -e "  ${YELLOW}[!]${RESET} $1"; }
fail()    { echo -e "  ${RED}[-]${RESET} $1"; }
detail()  { echo -e "  $1"; }

# ---------------------------------------------------------------------------
# Utility: extract JSON field (simple, no jq dependency fallback)
# ---------------------------------------------------------------------------
json_field() {
  # Usage: json_field "fieldName" < file_or_pipe
  local field="$1"
  if command -v jq &>/dev/null; then
    jq -r ".$field // empty" 2>/dev/null
  else
    python3 -c "import sys,json; d=json.load(sys.stdin); v=d.get('$field',''); print(v if v else '')" 2>/dev/null
  fi
}

# Count events in a response body
count_events() {
  local file="$1"
  if command -v jq &>/dev/null; then
    jq -r '.data | length' "$file" 2>/dev/null || echo "0"
  else
    python3 -c "import sys,json; d=json.load(open('$file')); print(len(d.get('data',[])))" 2>/dev/null || echo "0"
  fi
}

# Extract nextCursor from response
get_cursor() {
  local file="$1"
  if command -v jq &>/dev/null; then
    jq -r '.nextCursor // empty' "$file" 2>/dev/null
  else
    python3 -c "import sys,json; d=json.load(open('$file')); print(d.get('nextCursor',''))" 2>/dev/null
  fi
}

# Get rate limit remaining from headers file
get_rl_remaining() {
  local hfile="$1"
  grep -i "x-ratelimit-remaining\|ratelimit-remaining" "$hfile" 2>/dev/null | head -1 | sed 's/^[^:]*: *//' | tr -d '\r' || echo ""
}

get_rl_limit() {
  local hfile="$1"
  grep -i "x-ratelimit-limit\|ratelimit-limit" "$hfile" 2>/dev/null | head -1 | sed 's/^[^:]*: *//' | tr -d '\r' || echo ""
}

# Record a result for final summary
record_result() {
  # Usage: record_result "Strategy" "events_per_sec" "details"
  echo "$1|$2|$3" >> "$RESULTS_FILE"
}

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}=============================================================${RESET}"
echo -e "${BOLD}  PARALLEL / CONCURRENCY EXPLOITATION TEST SUITE${RESET}"
echo -e "${BOLD}=============================================================${RESET}"
echo "  Target: $API_V1"
echo "  Key:    ${API_KEY:0:8}...${API_KEY: -4}"
echo "  Time:   $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "  Goal:   Find maximum throughput for 3,000,000 event ingestion"
echo ""

# =====================================================================
# SECTION 1: Concurrency Scaling
# =====================================================================
banner "SECTION 1: Concurrency Scaling"
info "Testing N simultaneous requests to find max concurrency before 429s"
info "Concurrency levels: 5, 10, 20, 30, 50"

mkdir -p "$TMPDIR_P/s1"

for CONC in 5 10 20 30 50; do
  section "Concurrency = $CONC"
  mkdir -p "$TMPDIR_P/s1/c${CONC}"

  START_TIME=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")

  for i in $(seq 1 "$CONC"); do
    curl -s \
      -D "$TMPDIR_P/s1/c${CONC}/headers_$i" \
      -o "$TMPDIR_P/s1/c${CONC}/body_$i" \
      -w "%{http_code},%{time_total}" \
      -H "X-API-Key: $API_KEY" \
      "$API_V1/events?limit=100" \
      > "$TMPDIR_P/s1/c${CONC}/result_$i" 2>/dev/null &
  done
  wait

  END_TIME=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
  WALL_MS=$(( (END_TIME - START_TIME) / 1000000 ))

  # Analyze results
  OK_COUNT=0
  RATE_LIMITED=0
  OTHER_ERR=0
  TOTAL_TIME=0
  TOTAL_EVENTS=0

  for i in $(seq 1 "$CONC"); do
    RESULT=$(cat "$TMPDIR_P/s1/c${CONC}/result_$i" 2>/dev/null || echo "000,0")
    HTTP_CODE=$(echo "$RESULT" | cut -d',' -f1)
    REQ_TIME=$(echo "$RESULT" | cut -d',' -f2)

    if [ "$HTTP_CODE" = "200" ]; then
      OK_COUNT=$((OK_COUNT + 1))
      EVT=$(count_events "$TMPDIR_P/s1/c${CONC}/body_$i")
      TOTAL_EVENTS=$((TOTAL_EVENTS + EVT))
    elif [ "$HTTP_CODE" = "429" ]; then
      RATE_LIMITED=$((RATE_LIMITED + 1))
    else
      OTHER_ERR=$((OTHER_ERR + 1))
    fi

    # Accumulate time (integer math: multiply by 1000 for ms)
    TIME_MS=$(echo "$REQ_TIME" | awk '{printf "%d", $1 * 1000}')
    TOTAL_TIME=$((TOTAL_TIME + TIME_MS))
  done

  AVG_TIME=$((TOTAL_TIME / CONC))
  TOTAL_CALLS=$((TOTAL_CALLS + CONC))

  info "Results: ${OK_COUNT} OK, ${RATE_LIMITED} rate-limited (429), ${OTHER_ERR} errors"
  info "Wall-clock: ${WALL_MS}ms | Avg response: ${AVG_TIME}ms | Events fetched: ${TOTAL_EVENTS}"

  if [ "$RATE_LIMITED" -gt 0 ]; then
    # Show rate limit headers from a 429 response
    for i in $(seq 1 "$CONC"); do
      RESULT=$(cat "$TMPDIR_P/s1/c${CONC}/result_$i" 2>/dev/null || echo "000,0")
      HTTP_CODE=$(echo "$RESULT" | cut -d',' -f1)
      if [ "$HTTP_CODE" = "429" ]; then
        RETRY=$(grep -i "retry-after" "$TMPDIR_P/s1/c${CONC}/headers_$i" 2>/dev/null | tr -d '\r' || echo "")
        RL_REM=$(get_rl_remaining "$TMPDIR_P/s1/c${CONC}/headers_$i")
        warn "429 response - Retry-After: ${RETRY:-(none)} | Remaining: ${RL_REM:-(none)}"
        break
      fi
    done
  fi

  # Calculate effective events/sec
  if [ "$WALL_MS" -gt 0 ] && [ "$TOTAL_EVENTS" -gt 0 ]; then
    EPS=$(echo "$TOTAL_EVENTS $WALL_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
    info "Effective throughput: ${EPS} events/sec"
    record_result "Concurrency=$CONC" "$EPS" "${OK_COUNT}/${CONC} OK, ${WALL_MS}ms wall"
  else
    record_result "Concurrency=$CONC" "0" "${OK_COUNT}/${CONC} OK, ${WALL_MS}ms wall"
  fi

  # If all requests got rate-limited, stop escalating
  if [ "$OK_COUNT" -eq 0 ]; then
    warn "All requests rate-limited at concurrency=$CONC. Stopping escalation."
    warn "Waiting 5s for rate limit reset before next section..."
    sleep 5
    break
  fi

  # Brief pause between concurrency levels
  sleep 1
done

# =====================================================================
# SECTION 2: Multi-Cursor Pipeline
# =====================================================================
banner "SECTION 2: Multi-Cursor Pipeline"
info "Testing if multiple cursors can be consumed in parallel"

mkdir -p "$TMPDIR_P/s2"

# Step 1: Get first page and initial cursor
section "Collecting 5 cursors by sequential pagination"
CURSORS=()
CURSOR=""

for i in $(seq 1 5); do
  URL="$API_V1/events?limit=100"
  [ -n "$CURSOR" ] && URL="${URL}&cursor=${CURSOR}"

  HTTP_CODE=$(curl -s \
    -D "$TMPDIR_P/s2/cursor_headers_$i" \
    -o "$TMPDIR_P/s2/cursor_body_$i" \
    -w "%{http_code}" \
    -H "X-API-Key: $API_KEY" \
    "$URL")
  TOTAL_CALLS=$((TOTAL_CALLS + 1))

  if [ "$HTTP_CODE" != "200" ]; then
    fail "Page $i returned HTTP $HTTP_CODE - cannot collect more cursors"
    if [ "$HTTP_CODE" = "429" ]; then
      warn "Rate limited. Waiting 3s..."
      sleep 3
      # Retry once
      HTTP_CODE=$(curl -s -o "$TMPDIR_P/s2/cursor_body_$i" -w "%{http_code}" -H "X-API-Key: $API_KEY" "$URL")
      TOTAL_CALLS=$((TOTAL_CALLS + 1))
    fi
    [ "$HTTP_CODE" != "200" ] && break
  fi

  CURSOR=$(get_cursor "$TMPDIR_P/s2/cursor_body_$i")
  EVT=$(count_events "$TMPDIR_P/s2/cursor_body_$i")

  if [ -n "$CURSOR" ]; then
    CURSORS+=("$CURSOR")
    info "Page $i: got ${EVT} events, cursor: ${CURSOR:0:20}..."
  else
    info "Page $i: got ${EVT} events, no more cursors (end of data?)"
    break
  fi

  sleep 0.2
done

NUM_CURSORS=${#CURSORS[@]}
info "Collected $NUM_CURSORS cursors"

if [ "$NUM_CURSORS" -ge 2 ]; then
  # Step 2: Fire all cursors simultaneously
  section "Firing $NUM_CURSORS cursors in parallel"
  START_TIME=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")

  for idx in $(seq 0 $((NUM_CURSORS - 1))); do
    C="${CURSORS[$idx]}"
    curl -s \
      -D "$TMPDIR_P/s2/par_headers_$idx" \
      -o "$TMPDIR_P/s2/par_body_$idx" \
      -w "%{http_code},%{time_total}" \
      -H "X-API-Key: $API_KEY" \
      "$API_V1/events?limit=100&cursor=$C" \
      > "$TMPDIR_P/s2/par_result_$idx" 2>/dev/null &
    TOTAL_CALLS=$((TOTAL_CALLS + 1))
  done
  wait

  END_TIME=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
  WALL_MS=$(( (END_TIME - START_TIME) / 1000000 ))

  OK=0
  TOTAL_EVT=0
  for idx in $(seq 0 $((NUM_CURSORS - 1))); do
    RESULT=$(cat "$TMPDIR_P/s2/par_result_$idx" 2>/dev/null || echo "000,0")
    HTTP_CODE=$(echo "$RESULT" | cut -d',' -f1)
    REQ_TIME=$(echo "$RESULT" | cut -d',' -f2)

    if [ "$HTTP_CODE" = "200" ]; then
      OK=$((OK + 1))
      EVT=$(count_events "$TMPDIR_P/s2/par_body_$idx")
      TOTAL_EVT=$((TOTAL_EVT + EVT))
      NEW_CURSOR=$(get_cursor "$TMPDIR_P/s2/par_body_$idx")
      info "Cursor $idx: HTTP $HTTP_CODE | ${EVT} events | ${REQ_TIME}s | next: ${NEW_CURSOR:0:16}..."
    else
      fail "Cursor $idx: HTTP $HTTP_CODE | ${REQ_TIME}s"
    fi
  done

  info "Parallel cursor results: ${OK}/${NUM_CURSORS} OK | ${TOTAL_EVT} total events | ${WALL_MS}ms wall"

  if [ "$OK" -eq "$NUM_CURSORS" ]; then
    warn "ALL cursors consumed in parallel -- multi-cursor pipeline is VIABLE"
    EPS=$(echo "$TOTAL_EVT $WALL_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
    record_result "Multi-cursor (${NUM_CURSORS} parallel)" "$EPS" "${TOTAL_EVT} events, ${WALL_MS}ms"
  else
    info "Some cursors failed in parallel. May need cursor isolation."
    record_result "Multi-cursor (${NUM_CURSORS} parallel)" "partial" "${OK}/${NUM_CURSORS} OK"
  fi

  # Step 3: Test cursor conflict - use same cursor twice simultaneously
  section "Testing cursor conflict (same cursor used twice)"
  if [ "$NUM_CURSORS" -ge 1 ]; then
    C="${CURSORS[0]}"
    curl -s -o "$TMPDIR_P/s2/conflict_a" -w "%{http_code}" \
      -H "X-API-Key: $API_KEY" "$API_V1/events?limit=100&cursor=$C" \
      > "$TMPDIR_P/s2/conflict_a_code" 2>/dev/null &
    curl -s -o "$TMPDIR_P/s2/conflict_b" -w "%{http_code}" \
      -H "X-API-Key: $API_KEY" "$API_V1/events?limit=100&cursor=$C" \
      > "$TMPDIR_P/s2/conflict_b_code" 2>/dev/null &
    TOTAL_CALLS=$((TOTAL_CALLS + 2))
    wait

    CODE_A=$(cat "$TMPDIR_P/s2/conflict_a_code" 2>/dev/null)
    CODE_B=$(cat "$TMPDIR_P/s2/conflict_b_code" 2>/dev/null)
    info "Same cursor used twice: Response A = HTTP $CODE_A, Response B = HTTP $CODE_B"

    if [ "$CODE_A" = "200" ] && [ "$CODE_B" = "200" ]; then
      EVT_A=$(count_events "$TMPDIR_P/s2/conflict_a")
      EVT_B=$(count_events "$TMPDIR_P/s2/conflict_b")
      info "Both succeeded: A=${EVT_A} events, B=${EVT_B} events"
      if [ "$EVT_A" -eq "$EVT_B" ]; then
        info "Same event counts -- cursor is idempotent (safe to retry)"
      else
        warn "Different event counts -- cursor may be consumed on use"
      fi
    else
      warn "One or both failed -- cursor may be single-use"
    fi
  fi
else
  warn "Not enough cursors collected to test parallel consumption"
fi

sleep 1

# =====================================================================
# SECTION 3: Dual-Auth Sustained Throughput
# =====================================================================
banner "SECTION 3: Dual-Auth Sustained Throughput"
info "Testing if header-auth and query-auth have separate rate limit pools"

mkdir -p "$TMPDIR_P/s3"

section "Stream A: 10 sequential requests with X-API-Key header"
START_A=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
EVENTS_A=0
OK_A=0
RL_A=""

for i in $(seq 1 10); do
  HTTP_CODE=$(curl -s \
    -D "$TMPDIR_P/s3/ha_headers_$i" \
    -o "$TMPDIR_P/s3/ha_body_$i" \
    -w "%{http_code}" \
    -H "X-API-Key: $API_KEY" \
    "$API_V1/events?limit=100")
  TOTAL_CALLS=$((TOTAL_CALLS + 1))

  if [ "$HTTP_CODE" = "200" ]; then
    OK_A=$((OK_A + 1))
    EVT=$(count_events "$TMPDIR_P/s3/ha_body_$i")
    EVENTS_A=$((EVENTS_A + EVT))
  fi
  RL_A=$(get_rl_remaining "$TMPDIR_P/s3/ha_headers_$i")
done

END_A=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
TIME_A_MS=$(( (END_A - START_A) / 1000000 ))
info "Header auth: ${OK_A}/10 OK | ${EVENTS_A} events | ${TIME_A_MS}ms | RL remaining: ${RL_A:-(n/a)}"

section "Stream B: 10 sequential requests with ?apiKey= query param"
START_B=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
EVENTS_B=0
OK_B=0
RL_B=""

for i in $(seq 1 10); do
  HTTP_CODE=$(curl -s \
    -D "$TMPDIR_P/s3/qa_headers_$i" \
    -o "$TMPDIR_P/s3/qa_body_$i" \
    -w "%{http_code}" \
    "$API_V1/events?limit=100&apiKey=$API_KEY")
  TOTAL_CALLS=$((TOTAL_CALLS + 1))

  if [ "$HTTP_CODE" = "200" ]; then
    OK_B=$((OK_B + 1))
    EVT=$(count_events "$TMPDIR_P/s3/qa_body_$i")
    EVENTS_B=$((EVENTS_B + EVT))
  fi
  RL_B=$(get_rl_remaining "$TMPDIR_P/s3/qa_headers_$i")
done

END_B=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
TIME_B_MS=$(( (END_B - START_B) / 1000000 ))
info "Query auth: ${OK_B}/10 OK | ${EVENTS_B} events | ${TIME_B_MS}ms | RL remaining: ${RL_B:-(n/a)}"

section "Stream C: Both auth methods interleaved in parallel (10+10)"
START_C=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")

# Launch both streams simultaneously
(
  for i in $(seq 1 10); do
    curl -s -o "$TMPDIR_P/s3/dual_h_body_$i" -w "%{http_code}" \
      -H "X-API-Key: $API_KEY" "$API_V1/events?limit=100" \
      > "$TMPDIR_P/s3/dual_h_code_$i" 2>/dev/null
  done
) &
PID_H=$!

(
  for i in $(seq 1 10); do
    curl -s -o "$TMPDIR_P/s3/dual_q_body_$i" -w "%{http_code}" \
      "$API_V1/events?limit=100&apiKey=$API_KEY" \
      > "$TMPDIR_P/s3/dual_q_code_$i" 2>/dev/null
  done
) &
PID_Q=$!

wait $PID_H
wait $PID_Q
TOTAL_CALLS=$((TOTAL_CALLS + 20))

END_C=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
TIME_C_MS=$(( (END_C - START_C) / 1000000 ))

EVENTS_C_H=0; OK_C_H=0
EVENTS_C_Q=0; OK_C_Q=0
for i in $(seq 1 10); do
  CODE_H=$(cat "$TMPDIR_P/s3/dual_h_code_$i" 2>/dev/null || echo "000")
  CODE_Q=$(cat "$TMPDIR_P/s3/dual_q_code_$i" 2>/dev/null || echo "000")
  if [ "$CODE_H" = "200" ]; then
    OK_C_H=$((OK_C_H + 1))
    EVT=$(count_events "$TMPDIR_P/s3/dual_h_body_$i")
    EVENTS_C_H=$((EVENTS_C_H + EVT))
  fi
  if [ "$CODE_Q" = "200" ]; then
    OK_C_Q=$((OK_C_Q + 1))
    EVT=$(count_events "$TMPDIR_P/s3/dual_q_body_$i")
    EVENTS_C_Q=$((EVENTS_C_Q + EVT))
  fi
done
EVENTS_C=$((EVENTS_C_H + EVENTS_C_Q))

info "Dual-stream results: Header ${OK_C_H}/10 OK, Query ${OK_C_Q}/10 OK"
info "Total events: ${EVENTS_C} in ${TIME_C_MS}ms"

# Compare
SINGLE_BEST_TIME=$TIME_A_MS
SINGLE_BEST_EVENTS=$EVENTS_A
[ "$TIME_B_MS" -lt "$SINGLE_BEST_TIME" ] && SINGLE_BEST_TIME=$TIME_B_MS && SINGLE_BEST_EVENTS=$EVENTS_B

if [ "$TIME_C_MS" -gt 0 ] && [ "$EVENTS_C" -gt 0 ]; then
  EPS_SINGLE=$(echo "$SINGLE_BEST_EVENTS $SINGLE_BEST_TIME" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
  EPS_DUAL=$(echo "$EVENTS_C $TIME_C_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
  SPEEDUP=$(echo "$EPS_DUAL $EPS_SINGLE" | awk '{if($2>0) printf "%.2f", $1/$2; else print "N/A"}')
  info "Single-stream best: ${EPS_SINGLE} events/sec"
  info "Dual-stream:        ${EPS_DUAL} events/sec (${SPEEDUP}x speedup)"
  record_result "Single-stream (header)" "$EPS_SINGLE" "${OK_A}/10 OK"
  record_result "Dual-auth parallel" "$EPS_DUAL" "${SPEEDUP}x vs single"
fi

sleep 2

# =====================================================================
# SECTION 4: Multi-Worker Simulation
# =====================================================================
banner "SECTION 4: Multi-Worker Simulation"
info "Each worker: get page -> extract cursor -> get next page -> repeat (3 pages each)"

mkdir -p "$TMPDIR_P/s4"

# Worker function: paginate 3 pages, write results to file
run_worker() {
  local wid="$1"
  local auth_style="$2"  # "header" or "query"
  local pages=3
  local cursor=""
  local total_events=0
  local ok=0

  for p in $(seq 1 "$pages"); do
    URL="$API_V1/events?limit=100"
    [ -n "$cursor" ] && URL="${URL}&cursor=${cursor}"

    if [ "$auth_style" = "header" ]; then
      HTTP_CODE=$(curl -s -o "$TMPDIR_P/s4/w${wid}_p${p}_body" -w "%{http_code}" \
        -H "X-API-Key: $API_KEY" "$URL")
    else
      URL="${URL}&apiKey=$API_KEY"
      HTTP_CODE=$(curl -s -o "$TMPDIR_P/s4/w${wid}_p${p}_body" -w "%{http_code}" "$URL")
    fi

    if [ "$HTTP_CODE" = "200" ]; then
      ok=$((ok + 1))
      evt=$(count_events "$TMPDIR_P/s4/w${wid}_p${p}_body")
      total_events=$((total_events + evt))
      cursor=$(get_cursor "$TMPDIR_P/s4/w${wid}_p${p}_body")
    elif [ "$HTTP_CODE" = "429" ]; then
      # Wait briefly and retry
      sleep 1
      if [ "$auth_style" = "header" ]; then
        HTTP_CODE=$(curl -s -o "$TMPDIR_P/s4/w${wid}_p${p}_body" -w "%{http_code}" \
          -H "X-API-Key: $API_KEY" "$URL")
      else
        HTTP_CODE=$(curl -s -o "$TMPDIR_P/s4/w${wid}_p${p}_body" -w "%{http_code}" "$URL")
      fi
      if [ "$HTTP_CODE" = "200" ]; then
        ok=$((ok + 1))
        evt=$(count_events "$TMPDIR_P/s4/w${wid}_p${p}_body")
        total_events=$((total_events + evt))
        cursor=$(get_cursor "$TMPDIR_P/s4/w${wid}_p${p}_body")
      fi
    fi
  done

  echo "${wid}|${ok}|${total_events}" > "$TMPDIR_P/s4/w${wid}_result"
}

for WORKERS in 1 2 3 5; do
  section "Workers = $WORKERS"
  START_W=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")

  for w in $(seq 1 "$WORKERS"); do
    run_worker "$w" "header" &
  done
  wait

  END_W=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
  WALL_W_MS=$(( (END_W - START_W) / 1000000 ))

  TOTAL_EVT_W=0
  TOTAL_OK_W=0
  TOTAL_PAGES_W=$((WORKERS * 3))
  TOTAL_CALLS=$((TOTAL_CALLS + TOTAL_PAGES_W + TOTAL_PAGES_W / 3))  # estimate retries

  for w in $(seq 1 "$WORKERS"); do
    if [ -f "$TMPDIR_P/s4/w${w}_result" ]; then
      RES=$(cat "$TMPDIR_P/s4/w${w}_result")
      W_OK=$(echo "$RES" | cut -d'|' -f2)
      W_EVT=$(echo "$RES" | cut -d'|' -f3)
      TOTAL_OK_W=$((TOTAL_OK_W + W_OK))
      TOTAL_EVT_W=$((TOTAL_EVT_W + W_EVT))
      detail "  Worker $w: ${W_OK}/3 pages OK, ${W_EVT} events"
    fi
  done

  info "Total: ${TOTAL_OK_W}/${TOTAL_PAGES_W} pages OK | ${TOTAL_EVT_W} events | ${WALL_W_MS}ms wall"

  if [ "$WALL_W_MS" -gt 0 ] && [ "$TOTAL_EVT_W" -gt 0 ]; then
    EPS=$(echo "$TOTAL_EVT_W $WALL_W_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
    info "Throughput: ${EPS} events/sec"
    record_result "Workers=$WORKERS (3 pages each)" "$EPS" "${TOTAL_OK_W}/${TOTAL_PAGES_W} OK, ${WALL_W_MS}ms"
  fi

  # Clean up worker results for next round
  rm -f "$TMPDIR_P"/s4/w*_result

  sleep 1
done

sleep 2

# =====================================================================
# SECTION 5: Request Pipelining (Connection Reuse)
# =====================================================================
banner "SECTION 5: Request Pipelining (Connection Reuse)"
info "Comparing: reused connection vs new connections for 10 requests"

mkdir -p "$TMPDIR_P/s5"

section "10 requests with connection reuse (single curl, sequential URLs)"

# Build a config file for curl to fetch 10 pages in sequence on same connection
: > "$TMPDIR_P/s5/urls_reuse.txt"
for i in $(seq 1 10); do
  echo "url = \"$API_V1/events?limit=100\"" >> "$TMPDIR_P/s5/urls_reuse.txt"
  echo "output = \"$TMPDIR_P/s5/reuse_body_$i\"" >> "$TMPDIR_P/s5/urls_reuse.txt"
  echo "header = \"X-API-Key: $API_KEY\"" >> "$TMPDIR_P/s5/urls_reuse.txt"
  echo "" >> "$TMPDIR_P/s5/urls_reuse.txt"
done

START_R=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
curl -s -K "$TMPDIR_P/s5/urls_reuse.txt" \
  --keepalive-time 60 \
  -w "HTTP %{http_code} in %{time_total}s\n" \
  > "$TMPDIR_P/s5/reuse_timing.txt" 2>/dev/null
END_R=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
TIME_REUSE_MS=$(( (END_R - START_R) / 1000000 ))
TOTAL_CALLS=$((TOTAL_CALLS + 10))

EVENTS_REUSE=0
for i in $(seq 1 10); do
  if [ -f "$TMPDIR_P/s5/reuse_body_$i" ]; then
    EVT=$(count_events "$TMPDIR_P/s5/reuse_body_$i")
    EVENTS_REUSE=$((EVENTS_REUSE + EVT))
  fi
done

info "Connection reuse: ${EVENTS_REUSE} events in ${TIME_REUSE_MS}ms"

section "10 requests with fresh connections (separate curl invocations)"
START_F=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")

for i in $(seq 1 10); do
  curl -s -o "$TMPDIR_P/s5/fresh_body_$i" \
    -H "X-API-Key: $API_KEY" \
    -H "Connection: close" \
    "$API_V1/events?limit=100" 2>/dev/null
done

END_F=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
TIME_FRESH_MS=$(( (END_F - START_F) / 1000000 ))
TOTAL_CALLS=$((TOTAL_CALLS + 10))

EVENTS_FRESH=0
for i in $(seq 1 10); do
  if [ -f "$TMPDIR_P/s5/fresh_body_$i" ]; then
    EVT=$(count_events "$TMPDIR_P/s5/fresh_body_$i")
    EVENTS_FRESH=$((EVENTS_FRESH + EVT))
  fi
done

info "Fresh connections: ${EVENTS_FRESH} events in ${TIME_FRESH_MS}ms"

if [ "$TIME_REUSE_MS" -gt 0 ] && [ "$TIME_FRESH_MS" -gt 0 ]; then
  SPEEDUP=$(echo "$TIME_FRESH_MS $TIME_REUSE_MS" | awk '{if($2>0) printf "%.2f", $1/$2; else print "N/A"}')
  info "Connection reuse speedup: ${SPEEDUP}x"

  EPS_R=$(echo "$EVENTS_REUSE $TIME_REUSE_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
  EPS_F=$(echo "$EVENTS_FRESH $TIME_FRESH_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
  record_result "Conn reuse (10 seq)" "$EPS_R" "${EVENTS_REUSE} events, ${TIME_REUSE_MS}ms"
  record_result "Fresh conn (10 seq)" "$EPS_F" "${EVENTS_FRESH} events, ${TIME_FRESH_MS}ms"
fi

sleep 2

# =====================================================================
# SECTION 6: Burst Pattern Testing
# =====================================================================
banner "SECTION 6: Burst Pattern Testing"
info "Testing burst-and-wait patterns to find optimal burst size"

mkdir -p "$TMPDIR_P/s6"

for BURST in 5 10 20; do
  section "Burst size = $BURST"
  mkdir -p "$TMPDIR_P/s6/b${BURST}"

  START_BURST=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")

  # Fire burst
  for i in $(seq 1 "$BURST"); do
    curl -s \
      -D "$TMPDIR_P/s6/b${BURST}/headers_$i" \
      -o "$TMPDIR_P/s6/b${BURST}/body_$i" \
      -w "%{http_code}" \
      -H "X-API-Key: $API_KEY" \
      "$API_V1/events?limit=100" \
      > "$TMPDIR_P/s6/b${BURST}/code_$i" 2>/dev/null &
  done
  wait
  TOTAL_CALLS=$((TOTAL_CALLS + BURST))

  END_BURST=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
  BURST_MS=$(( (END_BURST - START_BURST) / 1000000 ))

  OK_BURST=0
  RL_BURST=0
  EVT_BURST=0
  RETRY_AFTER=""

  for i in $(seq 1 "$BURST"); do
    CODE=$(cat "$TMPDIR_P/s6/b${BURST}/code_$i" 2>/dev/null || echo "000")
    if [ "$CODE" = "200" ]; then
      OK_BURST=$((OK_BURST + 1))
      EVT=$(count_events "$TMPDIR_P/s6/b${BURST}/body_$i")
      EVT_BURST=$((EVT_BURST + EVT))
    elif [ "$CODE" = "429" ]; then
      RL_BURST=$((RL_BURST + 1))
      if [ -z "$RETRY_AFTER" ]; then
        RETRY_AFTER=$(grep -i "retry-after" "$TMPDIR_P/s6/b${BURST}/headers_$i" 2>/dev/null | sed 's/^[^:]*: *//' | tr -d '\r' || echo "")
      fi
    fi
  done

  info "Burst $BURST: ${OK_BURST} OK, ${RL_BURST} rate-limited | ${EVT_BURST} events | ${BURST_MS}ms"
  [ -n "$RETRY_AFTER" ] && info "Retry-After: ${RETRY_AFTER}s"

  # Calculate burst efficiency
  if [ "$BURST_MS" -gt 0 ] && [ "$EVT_BURST" -gt 0 ]; then
    EPS_BURST=$(echo "$EVT_BURST $BURST_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
    info "Burst throughput: ${EPS_BURST} events/sec"

    # If we had a retry-after, calculate sustained rate including wait
    if [ -n "$RETRY_AFTER" ] && [ "$RETRY_AFTER" -gt 0 ] 2>/dev/null; then
      TOTAL_CYCLE_MS=$((BURST_MS + RETRY_AFTER * 1000))
      EPS_SUSTAINED=$(echo "$EVT_BURST $TOTAL_CYCLE_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
      info "Sustained (burst + wait): ${EPS_SUSTAINED} events/sec"
      record_result "Burst=$BURST (with wait)" "$EPS_SUSTAINED" "${OK_BURST}/${BURST} OK, retry=${RETRY_AFTER}s"
    else
      record_result "Burst=$BURST" "$EPS_BURST" "${OK_BURST}/${BURST} OK, ${BURST_MS}ms"
    fi
  fi

  # Wait for rate limit reset between burst tests
  WAIT_SEC="${RETRY_AFTER:-2}"
  [ "$WAIT_SEC" -lt 1 ] 2>/dev/null && WAIT_SEC=1
  [ "$WAIT_SEC" -gt 10 ] 2>/dev/null && WAIT_SEC=5
  info "Waiting ${WAIT_SEC}s for rate limit reset..."
  sleep "$WAIT_SEC"
done

# =====================================================================
# SECTION 7: Mixed Strategy (Combine Best Techniques)
# =====================================================================
banner "SECTION 7: Mixed Strategy - Maximum Aggression"
info "Combining: dual-auth + parallel requests for 15 seconds"
info "(Reduced from 30s to conserve API calls)"

mkdir -p "$TMPDIR_P/s7"

MIXED_DURATION=15
MIXED_EVENTS=0
MIXED_CALLS=0
MIXED_429S=0

START_MIXED=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
END_TARGET=$(( $(date +%s) + MIXED_DURATION ))

ROUND=0
while [ "$(date +%s)" -lt "$END_TARGET" ]; do
  ROUND=$((ROUND + 1))

  # Fire a batch: 3 header-auth + 3 query-auth in parallel
  for i in 1 2 3; do
    curl -s -o "$TMPDIR_P/s7/h_${ROUND}_${i}" -w "%{http_code}" \
      -H "X-API-Key: $API_KEY" "$API_V1/events?limit=100" \
      > "$TMPDIR_P/s7/h_${ROUND}_${i}_code" 2>/dev/null &
    curl -s -o "$TMPDIR_P/s7/q_${ROUND}_${i}" -w "%{http_code}" \
      "$API_V1/events?limit=100&apiKey=$API_KEY" \
      > "$TMPDIR_P/s7/q_${ROUND}_${i}_code" 2>/dev/null &
  done
  wait
  MIXED_CALLS=$((MIXED_CALLS + 6))

  # Tally results
  for i in 1 2 3; do
    for PREFIX in h q; do
      CODE=$(cat "$TMPDIR_P/s7/${PREFIX}_${ROUND}_${i}_code" 2>/dev/null || echo "000")
      if [ "$CODE" = "200" ]; then
        EVT=$(count_events "$TMPDIR_P/s7/${PREFIX}_${ROUND}_${i}")
        MIXED_EVENTS=$((MIXED_EVENTS + EVT))
      elif [ "$CODE" = "429" ]; then
        MIXED_429S=$((MIXED_429S + 1))
      fi
    done
  done

  # If we're getting lots of 429s, back off briefly
  if [ "$MIXED_429S" -gt $((MIXED_CALLS / 3)) ]; then
    sleep 1
  else
    sleep 0.1
  fi

  # Safety: cap at ~50 rounds to stay within budget
  if [ "$MIXED_CALLS" -gt 150 ]; then
    warn "Approaching call budget limit, stopping mixed test"
    break
  fi
done

END_MIXED=$(date +%s%N 2>/dev/null || python3 -c "import time; print(int(time.time()*1e9))")
MIXED_MS=$(( (END_MIXED - START_MIXED) / 1000000 ))
TOTAL_CALLS=$((TOTAL_CALLS + MIXED_CALLS))

info "Mixed strategy: ${MIXED_EVENTS} events in ${MIXED_MS}ms"
info "API calls used: ${MIXED_CALLS} (${MIXED_429S} were 429s)"

if [ "$MIXED_MS" -gt 0 ] && [ "$MIXED_EVENTS" -gt 0 ]; then
  EPS_MIXED=$(echo "$MIXED_EVENTS $MIXED_MS" | awk '{printf "%.1f", $1 / ($2 / 1000)}')
  info "Mixed throughput: ${EPS_MIXED} events/sec"
  record_result "Mixed (dual-auth + parallel)" "$EPS_MIXED" "${MIXED_EVENTS} events, ${MIXED_CALLS} calls, ${MIXED_429S} 429s"
fi

# =====================================================================
# SECTION 8: Summary & Recommendations
# =====================================================================
banner "SECTION 8: RESULTS SUMMARY"
echo ""
echo -e "${BOLD}  Total API calls used: $TOTAL_CALLS${RESET}"
echo ""

printf "  ${BOLD}%-40s  %-15s  %s${RESET}\n" "STRATEGY" "EVENTS/SEC" "DETAILS"
printf "  %-40s  %-15s  %s\n" "----------------------------------------" "---------------" "----------------------------"

BEST_STRATEGY=""
BEST_EPS=0

while IFS='|' read -r strategy eps details; do
  [ -z "$strategy" ] && continue
  printf "  %-40s  %-15s  %s\n" "$strategy" "$eps" "$details"

  # Track best
  if [ "$eps" != "partial" ] && [ "$eps" != "0" ]; then
    IS_BETTER=$(echo "$eps $BEST_EPS" | awk '{if($1+0 > $2+0) print "yes"; else print "no"}')
    if [ "$IS_BETTER" = "yes" ]; then
      BEST_EPS="$eps"
      BEST_STRATEGY="$strategy"
    fi
  fi
done < "$RESULTS_FILE"

echo ""
echo -e "${BOLD}${GREEN}  RECOMMENDED STRATEGY: $BEST_STRATEGY${RESET}"
echo -e "${BOLD}${GREEN}  Best throughput:      $BEST_EPS events/sec${RESET}"
echo ""

# Project time to ingest 3M events
if [ -n "$BEST_EPS" ] && [ "$BEST_EPS" != "0" ]; then
  PROJECTED_SEC=$(echo "$BEST_EPS" | awk '{if($1+0 > 0) printf "%.0f", 3000000 / $1; else print "N/A"}')
  PROJECTED_MIN=$(echo "$PROJECTED_SEC" | awk '{printf "%.1f", $1 / 60}')
  echo -e "  ${BOLD}Projected time to ingest 3,000,000 events:${RESET}"
  echo -e "  ${BOLD}  ${PROJECTED_SEC} seconds = ${PROJECTED_MIN} minutes${RESET}"
  echo ""

  # Show projections for all strategies
  echo -e "  ${BOLD}Projection table (3M events):${RESET}"
  printf "  %-40s  %-15s  %s\n" "STRATEGY" "EVENTS/SEC" "TIME TO 3M"
  printf "  %-40s  %-15s  %s\n" "----------------------------------------" "---------------" "----------"

  while IFS='|' read -r strategy eps details; do
    [ -z "$strategy" ] && continue
    if [ "$eps" != "partial" ] && [ "$eps" != "0" ]; then
      T_SEC=$(echo "$eps" | awk '{if($1+0 > 0) printf "%.0f", 3000000 / $1; else print "N/A"}')
      T_MIN=$(echo "$T_SEC" | awk '{if($1+0 > 0) printf "%.1f min", $1 / 60; else print "N/A"}')
      printf "  %-40s  %-15s  %s\n" "$strategy" "$eps" "$T_MIN"
    fi
  done < "$RESULTS_FILE"
fi

echo ""
echo -e "${BOLD}  KEY FINDINGS:${RESET}"
echo "  - Concurrency: Check Section 1 for max parallel requests before 429"
echo "  - Cursors: Check Section 2 for multi-cursor viability"
echo "  - Dual-auth: Check Section 3 for separate rate limit pools"
echo "  - Workers: Check Section 4 for optimal worker count"
echo "  - Pipelining: Check Section 5 for connection reuse benefit"
echo "  - Burst: Check Section 6 for optimal burst size"
echo "  - Mixed: Check Section 7 for combined strategy throughput"
echo ""
echo -e "${BOLD}  IMPLEMENTATION RECOMMENDATIONS:${RESET}"
echo "  1. Use the worker count from Section 4 that gave highest throughput"
echo "  2. If dual-auth works (Section 3), split workers across both auth methods"
echo "  3. If multi-cursor works (Section 2), pre-fetch cursors to seed workers"
echo "  4. Use connection pooling / keep-alive in TypeScript HTTP client"
echo "  5. Implement burst-and-backoff if rate limits are strict (Section 6)"
echo ""
echo -e "${BOLD}=============================================================${RESET}"
echo -e "${BOLD}  Test Complete: $(date -u '+%Y-%m-%dT%H:%M:%SZ')${RESET}"
echo -e "${BOLD}  Total API calls: $TOTAL_CALLS${RESET}"
echo -e "${BOLD}=============================================================${RESET}"
