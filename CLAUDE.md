# DataSync Ingestion Challenge - Context for Claude Sessions

## GOAL
Ingest **3,000,000 events** from a remote API into PostgreSQL and submit all event IDs.
- Scoring: **60% API Discovery & Throughput**, 40% Architecture.
- Top candidates finish in **under 30 minutes** total (from first API call to submission).
- API key is valid for **3 hours from first use**. Timer starts on first API call.
- Max **5 submissions** allowed — each POST to `/submissions` counts, even if it fails.
- **Verification requirement:** Solution must work from scratch on a clean Linux machine with only Docker installed. Must be fully automated — no manual intervention, pauses, or restarts.
- **"If you feel limited by the API, keep pushing. There's always a faster way."**

## CORE REQUIREMENTS (from README)
1. **Connect** to the DataSync API
2. **Extract** ALL events from the system (3,000,000)
3. **Handle** the API's pagination correctly
4. **Respect rate limits** — read rate limit headers, back off on 429, pace within budget. **Never intentionally trigger 429.**
5. **Store data** in PostgreSQL
6. **Make it resumable** — save progress, resume after failure

## SHOULD HAVE (from README)
- **Throughput optimization** — maximize events per second (implemented: adaptive concurrency, rate budget pacer, dual-auth rotation)
- **Progress tracking** — show ingestion progress (implemented: `logProgress()` every 10K events — shows %, evt/s, ETA, queue lag)
- **Health checks** — monitor worker health (implemented: health watchdog every 10s — active/stuck/dead workers, queue depth)

## DISCOVERY MINDSET (critical — 60% of score)
The API documentation is **minimal by design**. The challenge is discovering how it actually works.

**Philosophy: observe everything, assume nothing.**
- Every API response is a source of information. Log and inspect ALL response headers, body structure, field names, value types, and formats — especially on the first few requests.
- Don't hardcode assumptions about field names (e.g. `id` vs `eventId` vs `_id`), timestamp formats (ISO 8601 vs Unix seconds vs ms), or pagination structure. Detect them empirically.
- **"Curious developers explore everything, even data model by looking at dashboard and inspect calls"** — the code fetches the dashboard HTML, parses JS bundles for `fetch()` URLs, and deeply inspects `/internal/stats`.
- The `probeApiShape()` function in discover.ts runs first and logs the full shape of API responses.
- The `logFirstRequest()` function in api.ts logs complete details for the first 3 API requests.
- The `logEventSchema()` function in ingest.ts logs field names, types, and sample values from the first batch of events.
- The `probeDashboardAndBundle()` function fetches dashboard HTML, finds JS bundles, and scans them for API endpoints, query params, and headers.

**Key hints from the challenge:**
- **Pay attention to response headers** — rate limit info, hints about other endpoints
- **The API has behaviors that aren't documented** — probe endpoints, test parameters
- **Timestamp formats may vary across responses** — normalize carefully
- **"The documented API may not be the fastest way"** — undocumented fast-path endpoints exist
- **"Good engineers explore every corner of an application"** — dashboard, HTML source, JS bundles
- **"Cursors have a lifecycle"** — they expire (~60 seconds), don't let them go stale
- **"If you feel limited by the API, keep pushing. There's always a faster way."**

---

## SUBMISSION — CRITICAL SAFETY RULES

**Only 5 submission attempts per API key.** Every POST to `/submissions` counts, even if it returns an error.

### Safety guards implemented in submit.ts:
1. **Hard block if < 3M events** — refuses to submit, logs error, returns without hitting the API
2. **Verifies count twice** — exact DB count AND file line count must both be >= 3M
3. **Checks remaining submissions** — GETs `/submissions` first to see how many attempts remain
4. **No retry on HTTP responses** — if the server responds (any status code), the attempt is consumed; do NOT retry
5. **Only retries on network errors** — connection refused, timeout, DNS failure (server never received the request)

### Submission format:
```bash
# Option 1: Auto-submit (set env vars before running)
AUTO_SUBMIT=true GITHUB_REPO=https://github.com/user/repo sh run-ingestion.sh

# Option 2: Manual submit after ingestion completes
docker exec assignment-postgres psql -U postgres -d ingestion -t -A -c "SELECT id FROM ingested_events" > event_ids.txt
curl -X POST \
  -H "X-API-Key: $TARGET_API_KEY" \
  -H "Content-Type: text/plain" \
  --data-binary @event_ids.txt \
  "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions?github_repo=https://github.com/user/repo"
```

### Expected response:
```json
{
  "success": true,
  "data": {
    "submissionId": "uuid",
    "eventCount": 3000000,
    "githubRepoUrl": "https://github.com/user/repo",
    "timeToSubmit": { "ms": 1234567, "minutes": 20.6, "formatted": "20m 35s" },
    "submissionNumber": 1,
    "remainingSubmissions": 4
  }
}
```

---

## RUNNING THE SOLUTION (fully automated e2e)
```bash
# Set API key, then run:
export TARGET_API_KEY="your-key-here"
sh run-ingestion.sh
```
- `run-ingestion.sh` runs `docker compose up -d --build`, waits for init, then monitors progress
- It checks for `"ingestion complete"` in container logs (exact string, lowercase — logged by `index.ts`)
- It queries `SELECT reltuples FROM pg_class WHERE relname = 'ingested_events'` for progress
- The entire flow (DB init → discovery → ingestion → completion) is fully automated

## DO NOT DELETE DOCKER VOLUMES
- **NEVER** run `docker compose down -v` — the `-v` flag destroys volumes
- `ingestion_wal` volume = write-ahead log of fetched event IDs (crash recovery)
- `assignment_postgres_data` volume = PostgreSQL data
- Safe: `docker compose down`, `docker compose restart`, `docker compose up -d --build`

---

## API REFERENCE

| Field | Value |
|-------|-------|
| Base URL | `http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1` |
| Dashboard | `http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com` |
| Auth (preferred) | `X-API-Key` header — gets **better rate limits** than query param |
| Auth (alternate) | `?api_key=KEY` query param (underscore, NOT `apiKey`) |
| Server | Nginx 1.29.4 → Express → PostgreSQL + Redis |

### Key Endpoints
| Endpoint | Method | Auth | Notes |
|----------|--------|------|-------|
| `/api/v1/events` | GET | Yes | `?limit=N&cursor=CURSOR`. Response: `{ data: [...], hasMore, nextCursor }` |
| `/api/v1/events/all` | GET | Yes | May return all events at once |
| `/api/v1/events/ids` | GET | Yes | May return IDs only (fastest) |
| `/api/v1/events/export` | GET | Yes | Fast path |
| `/api/v1/events/download` | GET | Yes | Fast path |
| `/api/v1/events/stream` | GET | Yes | SSE stream (`text/event-stream`) |
| `/api/v1/events/sse` | GET | Yes | Server-Sent Events |
| `/api/v1/events/batch` | GET | Yes | Batch with offset support |
| `/api/v1/events/bulk` | POST | Yes | `{ ids: [...] }` — fetch specific events by ID |
| `/api/v1/events/count` | GET | Yes | Event count |
| `/api/v1/submissions` | GET/POST | Yes | GET: check submissions. POST: submit IDs (text/plain, one per line) + `?github_repo=URL` |
| `/internal/dashboard/stream-access` | POST | Yes | Returns `{ streamAccess: { endpoint, token, tokenHeader, expiresIn } }` |
| `/internal/stats` | GET | No | System stats (counts, distributions, cache info) |
| `/internal/health` | GET | No | DB + Redis health |

### Data Model (from /internal/stats)
- **3,000,000 events** across **8 types**: page_view (1.05M), click (749K), api_call (300K), form_submit (300K), scroll (150K), purchase (150K), error (150K), video_play (150K)
- **3 device types**: mobile, desktop, tablet
- **3,000 users**, **60,000 sessions**

### Rate Limiting
- **Always respect rate limits.** Read `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` headers.
- Back off on HTTP 429 responses. Use `Retry-After` header when present.
- Header auth (`X-API-Key`) gets better rate limits than query param auth (`?api_key=`).
- The two auth methods may have **separate rate limit pools** — using both legitimately increases throughput.
- The code measures rate limit budgets at startup and paces requests to stay within limits (never intentionally triggers 429).

### Throughput Math
- 3M events / 30 min = 100K events/min = 1,667 events/sec minimum
- Naive pagination at 100/page = 30K API calls = too slow even at 10 req/sec
- Must discover fast-path endpoints or use large page sizes (10K+) to meet the target

---

## SOLUTION ARCHITECTURE

### Environment Variables (set in docker-compose.yml)
| Var | Default | Purpose |
|-----|---------|---------|
| `TARGET_API_KEY` | (required) | API authentication key |
| `WORKER_CONCURRENCY` | 10 | Number of parallel workers |
| `MAX_IN_FLIGHT` | 40 | Max concurrent API requests (adaptive — adjusts at runtime) |
| `BATCH_SIZE` | 1000 | DB batch insert size |
| `ID_ONLY` | `"true"` | Store only event IDs (not full JSON) — minimizes storage/IO |
| `AUTO_SUBMIT` | `"false"` | Auto-submit results when ingestion completes. **Set to `"true"` with `GITHUB_REPO` for fully automated flow.** |
| `GITHUB_REPO` | (empty) | GitHub repo URL for submission |
| `WAL_DIR` | `/data` | Directory for write-ahead log file |
| `NODE_OPTIONS` | `--max-old-space-size=2048` | Memory limit |

### Project Structure
```
packages/ingestion/src/
  index.ts      - Entry point: DB init → API validation → ingestion → submission
  discover.ts   - Parallel API discovery (probes endpoints, page sizes, rate limits, dashboard, JS bundles)
  ingest.ts     - Ingestion engine with 6 strategy layers + adaptive concurrency + health watchdog
  api.ts        - HTTP client: dual-auth, rate limiting, connection pooling, throughput tracking
  db.ts         - PostgreSQL: batch inserts via unnest(), UNLOGGED table, ID-only mode
  submit.ts     - Submission with safety guards: blocks if < 3M, no retry on HTTP responses
```

### DB Schema (actual, with ID_ONLY=true default)
```sql
-- UNLOGGED for speed (no WAL overhead), ID-only (no data JSONB column)
CREATE TABLE ingested_events (
  id TEXT PRIMARY KEY
) WITH (fillfactor = 100, autovacuum_enabled = false);

CREATE TABLE ingestion_progress (
  id SERIAL PRIMARY KEY, cursor TEXT, events_count BIGINT,
  status TEXT, strategy TEXT, metadata JSONB, updated_at TIMESTAMPTZ
);
```

### Container Names (hardcoded in docker-compose.yml and run-ingestion.sh)
- `assignment-ingestion` — the ingestion service
- `assignment-postgres` — PostgreSQL database

### Crash Recovery
1. **Write-Ahead Log (WAL):** Every fetched batch of IDs is appended to `/data/ingested_ids.wal` BEFORE DB insert. On restart, `recoverFromWal()` replays the WAL to fill any gaps.
2. **DB progress table:** Cursor/offset checkpointed every 50K events.
3. **`restart: on-failure:3`** in docker-compose.yml for automatic restart.

---

## INGESTION STRATEGY (as implemented in ingest.ts)

**Priority order — first working strategy wins:**

1. **`/events/ids` or `/events/all`** — if either returns all 3M events at once, done instantly
2. **Parallel offset pagination** — if `?offset=N` works, shard into N workers with zero overlap (cleanest)
3. **Stream + parallel combo** — stream endpoint (own rate limit pool) + cursor/type workers simultaneously
4. **Parallel by event type** — 8 workers, each filtering by one event type (if `?type=` param works)
5. **Parallel cursor chains** — N staggered cursor chains paginating independently
6. **Single pipelined cursor** — fallback, single cursor with large page sizes

### Discovery Phase (discover.ts)
Runs all probes **in parallel** at startup:
- **Deep API inspection:** `probeApiShape()` — logs ALL headers, body shape, event schema, ID/timestamp detection, cursor format
- **Dashboard & JS bundle analysis:** `probeDashboardAndBundle()` — fetches dashboard HTML, parses `<script>` tags, scans JS for `fetch()` URLs, API paths, query params, headers
- **Data model inspection:** deep `/internal/stats` logging — all counts, distributions, cache info, unexpected fields
- Page size probing: tests limit=5000, 10000, 20000, 50000, 100000
- Fast-path endpoints: /events/all, /events/ids, /events/batch
- Stream access: POST /internal/dashboard/stream-access
- Dual-auth rate limit pools: header vs query param
- Offset pagination: tests ?offset=, ?page=, ?skip=
- Rate limit budget measurement: measures pool sizes, windows, whether separate
- Custom header probes: X-Batch-Mode, X-Priority, X-Internal, etc.
- SSE probe: /events/sse with Accept: text/event-stream
- Time-based param probes: ?since=, ?from=, ?until=, etc.
- Type filtering: ?type=page_view, ?eventType=click
- Content type negotiation: NDJSON, CSV, sparse fields
- Response header inspection: Link headers, stream hints

### Runtime Features
- **Adaptive concurrency:** Every 5s, measures throughput. Ramps up concurrency if improving, backs off if degrading.
- **Rate budget pacer:** Computes optimal request interval from measured rate limits. Stays within budget to avoid 429s.
- **Throughput tracker:** Records events/sec per strategy. Reports best strategy at completion.
- **Health watchdog:** Every 10s, reports active/stuck/dead workers, queue depth, lag, overall rate.
- **Dual-auth rotation:** Alternates header and query auth if they have separate rate limit pools.
- **Async insert queue:** Workers push IDs to queue, background flusher writes to DB — decouples API fetches from DB writes.

---

## WHAT TO IMPROVE NEXT

If throughput is insufficient, investigate:
1. **Are there undiscovered fast-path endpoints?** Check discovery logs for new endpoints found in JS bundles.
2. **Is the stream endpoint working?** Check if `stream-access` returns a token and what throughput it achieves.
3. **Are page sizes maxed out?** If limit=100000 works, that's 100x fewer requests.
4. **Is offset pagination available?** Zero-overlap parallel sharding is the cleanest approach.
5. **Are rate limits the bottleneck?** Check if the pacer is throttling. If dual pools are separate, both should be saturated.
6. **Is the DB the bottleneck?** Check if the insert queue lag is growing. Consider increasing batch size.
7. **Review the full discovery log output** — it logs everything found. Look for patterns or hints.

---

## SCRIPTS AVAILABLE
| Script | Purpose |
|--------|---------|
| `sh run-ingestion.sh` | Run full ingestion via Docker (primary entry point) |
| `sh discover.sh` | Quick API discovery (requires `API_KEY` env var) |
| `sh test-internal.sh` | Probe /internal/ endpoints |
| `sh test-discovery.sh` | Comprehensive discovery |
| `sh test-ratelimits.sh` | Rate limit measurement tests |
| `sh test-cursors.sh` | Cursor behavior tests |
| `sh test-parallel.sh` | Parallel ingestion tests |
| `sh test-payload.sh` | Payload optimization tests |
