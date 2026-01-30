# DataSync Ingestion Challenge - Solution

## Requirements Checklist

### Must Have
- [x] **TypeScript codebase** - All source in `packages/ingestion/src/` (index.ts, discover.ts, ingest.ts, api.ts, db.ts, submit.ts)
- [x] **PostgreSQL for data storage** - UNLOGGED table with ID-only schema for max throughput
- [x] **Docker Compose** - `docker compose up -d --build` runs everything; `sh run-ingestion.sh` is the single entry point
- [x] **Proper error handling and logging** - Structured `[module]` prefixed logs, graceful shutdown on SIGTERM/SIGINT, retry with exponential backoff on transient errors
- [x] **Rate limit handling** - Reads `X-RateLimit-Remaining/Reset` headers, backs off on 429, paces requests within budget, never intentionally triggers 429
- [x] **Resumable ingestion** - File-based WAL + DB progress checkpoints + `ON CONFLICT DO NOTHING` idempotent inserts; Docker `restart: on-failure:3`

### Should Have
- [x] **Throughput optimization** - Adaptive concurrency, dual-auth rate limit pool rotation, async insert queue with 8 parallel flushers, feed endpoint discovery for higher throughput
- [x] **Progress tracking** - Logs every 10K events: %, evt/s, ETA, queue lag
- [x] **Health checks** - Watchdog every 10s: active/stuck/dead workers, queue depth, overall rate

### Nice to Have
- [ ] Unit tests - Not implemented (see "What I Would Improve" below)
- [ ] Integration tests - Not implemented
- [ ] Metrics/monitoring - Basic console logging; no Prometheus/Grafana
- [x] Architecture documentation - This README

---

## How to Run

### Prerequisites
- Docker and Docker Compose installed
- A valid API key from the interviewer

### Quick Start
```bash
export TARGET_API_KEY="your-api-key-here"
sh run-ingestion.sh
```

This single command:
1. Builds and starts PostgreSQL + the ingestion service via Docker Compose
2. Runs API discovery, then parallel ingestion
3. Monitors progress every 5 seconds until completion
4. Logs "ingestion complete" when all 3M events are stored

### Manual Submission (after ingestion completes)
```bash
docker exec assignment-postgres psql -U postgres -d ingestion -t -A \
  -c "SELECT id FROM ingested_events" > event_ids.txt

curl -X POST \
  -H "X-API-Key: $TARGET_API_KEY" \
  -H "Content-Type: text/plain" \
  --data-binary @event_ids.txt \
  "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions?github_repo=https://github.com/mayureshjakhotia/data-sync-ingestion-challenge"
```

### Auto-Submit Mode
```bash
export TARGET_API_KEY="your-api-key"
export AUTO_SUBMIT=true
export GITHUB_REPO=https://github.com/mayureshjakhotia/data-sync-ingestion-challenge
sh run-ingestion.sh
```

---

## Architecture Overview

### Project Structure
```
packages/ingestion/src/
  index.ts      - Entry point: DB init -> API validation -> ingestion -> submission
  discover.ts   - Parallel API discovery (probes endpoints, page sizes, rate limits, dashboard, JS bundles)
  ingest.ts     - Ingestion engine with 6 strategy layers + adaptive concurrency + health watchdog
  api.ts        - HTTP client: dual-auth, rate limiting, connection pooling, throughput tracking
  db.ts         - PostgreSQL: batch inserts via unnest(), UNLOGGED table, ID-only mode
  submit.ts     - Submission with safety guards (blocks if < 3M, no retry on HTTP responses)
```

### Strategy Layers (auto-detected, fastest first)

The ingestion engine tries strategies in priority order:

1. **`/events/all` or `/events/ids`** - Instant if available (returns all 3M at once)
2. **Feed endpoint** (`/events/d4ta/x7k9/feed`) - Discovered via dashboard JS bundle analysis. Has its own rate limit pool with higher limits (~1.6MB / 5000 events per request). This was the primary strategy that ingested the majority of events.
3. **Dual-pool cursor chains** - Two workers using separate auth methods (header vs query param), each with independent rate limit pools
4. **Parallel by event type** - 8 workers filtering by event type (`?type=page_view`, etc.)
5. **Parallel cursor chains** - N staggered cursors paginating independently
6. **Single pipelined cursor** - Fallback with large page sizes

### Key Design Decisions

**ID-Only Storage:** We only need event IDs for submission, so the DB stores just `id TEXT PRIMARY KEY` -- no JSON payload. This minimizes storage, IO, and insert time.

**UNLOGGED Table:** PostgreSQL's WAL is disabled for the ingested_events table since we can re-ingest on crash. Combined with `synchronous_commit = OFF`, this maximizes insert throughput.

**Batch Inserts via `unnest()`:** Instead of multi-row VALUES with thousands of parameters, we use `INSERT INTO ... SELECT unnest($1::text[])` which accepts a single array parameter -- no 65535-param limit, faster parsing.

**Async Insert Queue:** API fetch workers push IDs to an in-memory queue. 8 parallel DB flusher coroutines drain the queue in the background, decoupling API latency from DB latency.

**Dual-Auth Rate Limit Pools:** The API has separate rate limits for header auth (`X-API-Key`, 10 req/60s) and query param auth (`?api_key=`, 5 req/60s). Using both simultaneously increases total throughput.

**Adaptive Concurrency:** Every 5 seconds, the system measures throughput. If improving, it ramps up concurrent requests. If degrading, it backs off.

**Write-Ahead Log:** Every fetched batch of IDs is appended to a file-based WAL before DB insert. On restart, `recoverFromWal()` replays any IDs that didn't make it to the DB.

### Crash Recovery

1. WAL file at `/data/ingested_ids.wal` preserves fetched IDs across restarts
2. `ingestion_progress` table checkpoints cursor position every 50K events
3. Docker `restart: on-failure:3` auto-restarts on crash
4. `ON CONFLICT DO NOTHING` makes all inserts idempotent

### Submission Safety Guards

Only 5 attempts per API key -- the code enforces:
- Hard block if DB has < 3,000,000 events
- Verifies count via exact `COUNT(*)` (not estimated)
- Checks remaining submissions via `GET /submissions` before POSTing
- No retry on HTTP responses (any response = attempt consumed)
- Only retries on network errors (connection refused, timeout)

---

## API Discoveries

### Documented Behavior
- **Base URL:** `http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1`
- **Pagination:** Cursor-based, descending by timestamp. `{ data: [...], pagination: { hasMore, nextCursor, cursorExpiresIn } }`
- **Max page size:** 5000 (server caps regardless of `limit` param)
- **Cursor expiry:** ~119 seconds. Stale cursors return `400 Bad Request`
- **3,000,000 events** across 8 types: page_view (1.05M), click (749K), api_call (300K), form_submit (300K), scroll (150K), purchase (150K), error (150K), video_play (150K)
- **3 device types:** mobile, desktop, tablet
- **3,000 users, 60,000 sessions**

### Undocumented Discoveries
- **Dual rate limit pools:** Header auth (`X-API-Key`) and query param auth (`?api_key=`) have **separate** rate limit budgets. Header: 10 req/60s. Query: 5 req/60s.
- **Feed endpoint:** `POST /internal/dashboard/stream-access` with `X-Dashboard-Session: true` header + Origin/Referer from the dashboard URL returns a stream token and feed URL (`/api/v1/events/d4ta/x7k9/feed`). This endpoint has higher rate limits than the standard API.
- **`X-RateLimit-Reset` is relative:** The reset header is seconds-until-reset, not an epoch timestamp.
- **`/events/bulk` endpoint:** `POST /api/v1/events/bulk` with `{ ids: [...] }` returns specific events and a `missing` array -- useful for verification.
- **`/internal/stats`:** Requires API key auth. Returns event counts, type distributions, device distributions, cache info.
- **`/internal/health`:** No auth needed. Returns DB and Redis health status.
- **JS bundle analysis:** Dashboard HTML contains `<script>` tags pointing to JS bundles. Scanning these for `fetch()` calls revealed the `/internal/dashboard/stream-access` endpoint and the feed URL pattern.
- **Timestamp format varies:** Some responses return Unix milliseconds, others return ISO 8601 strings. The `timestampMs` field is always `null`.
- **Endpoints that don't exist:** `/events/ids`, `/events/batch`, `/events/export`, `/events/download`, `/events/stream`, `/events/sse`, `/events/count` all return 404 (treated as event ID lookup by the router).
- **Type filtering works:** `?type=page_view` correctly filters events by type.
- **Offset pagination doesn't work:** `?offset=N` is ignored; the API only supports cursor-based pagination.
- **Server stack:** Nginx 1.29.4 -> Express -> PostgreSQL + Redis

---

## What Happened During Ingestion

The feed endpoint (`/events/d4ta/x7k9/feed`) successfully ingested **2,957,456 of 3,000,000 events (98.6%)** in the first pass. The remaining ~42,544 events were the oldest events in the dataset -- the feed paginates newest-to-oldest, and all 3 feed workers had overlapping coverage that missed the tail.

On resume, the feeds had to restart from the newest event and re-paginate through 2.95M duplicates (rejected by `ON CONFLICT DO NOTHING`) to reach the ~42K oldest events. This eventually completed, bringing the total to **3,000,003 unique events** which were successfully submitted.

**Lesson learned:** Storing only event IDs (no timestamps) made resumption inefficient. Storing `(id, timestamp)` would have allowed targeted fetching of the missing date range via `?since=` and `?until=` params, avoiding the duplicate grind entirely.

**Open question:** The final DB count was 3,000,003 -- 3 more than the 3,000,000 reported by `/internal/stats` and `meta.total`. Since every row is unique (enforced by `PRIMARY KEY`), these are genuinely distinct IDs returned by the API. This likely points to a bug in the ingestion pipeline -- possibly cursor boundary pages returning an overlapping event that has a different ID than expected, or an edge case in `extractId()` parsing a non-ID field as an ID. With more time, this would warrant investigation: fetch those 3 extra IDs, look them up via `/events/bulk`, and verify they are real events.

---

## What I Would Improve With More Time

1. **Don't hardcode the 3M target:** The solution hardcodes `TOTAL_EVENTS = 3_000_000` and blocks submission if count < 3M. In practice, the API returned 3,000,003 unique events -- more than `/internal/stats` reported. A better approach: fetch the total from `meta.total` in the API response dynamically, and use that as the target.

2. **Store timestamps alongside IDs:** Store `(id TEXT, timestamp BIGINT)` instead of just `id`. On resume, query `SELECT MIN(timestamp), MAX(timestamp)` to identify the missing date range, then use `?since=` and `?until=` to fetch only the gap.

3. **Smarter gap detection:** When stuck at 98.6% (feeds grinding through duplicates), implement a binary search approach: fetch pages at the tail end of the cursor chain, detect where new events start, and focus fetching there.

4. **Cursor chain handoff:** When a cursor chain finishes its segment, hand off its rate limit budget to a chain that's still working -- avoid wasting rate limit capacity.

5. **Parallel feed workers with time partitioning:** Partition the full date range (Dec 25, 2025 - Jan 27, 2026) into non-overlapping time slices, one worker per slice, for zero-overlap parallel ingestion.

6. **Pre-computed ID set for dedup:** Load all DB IDs into a Set in memory at startup. Check incoming IDs against this set before enqueueing -- avoid DB roundtrips for known duplicates.

7. **Streaming submission:** Instead of writing all 3M IDs to a file then reading it back, stream directly from the DB cursor to the HTTP POST body.

8. **Validate IDs are UUIDs:** The current `extractId()` function blindly stores whatever string it finds in the `id` field. Adding a UUID format check (e.g., regex `/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i`) would catch malformed IDs before they reach the DB -- preventing wasted rows and potential submission mismatches.

9. **Unit and integration tests:** Test cursor expiry handling, rate limit backoff, WAL recovery, and the submission safety guards. Critically, add tests that verify the correct event ID is being extracted and stored -- e.g., fetch a known batch via `/events/bulk`, insert into the DB, then query back and assert the IDs match exactly. The 3,000,003 count (vs expected 3,000,000) suggests a possible edge case in ID extraction or cursor boundary handling that tests would catch.

9. **Metrics and monitoring:** Prometheus metrics for events/sec, rate limit utilization, queue depth, and DB insert latency. Grafana dashboard for real-time visibility.

---

## Tools Used

- **Claude Code (Claude Opus 4.5)** - AI coding assistant used for architecture design, API discovery strategy, code implementation, and debugging throughout the challenge.
