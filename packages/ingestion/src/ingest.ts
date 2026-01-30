/**
 * Aggressive parallel ingestion engine.
 *
 * Strategy layers (auto-detected, fastest first):
 *  -2: /events/all or /events/ids (instant if available)
 *  -1: Stream endpoint (separate rate limit pool)
 *   0: Bulk/export endpoint
 *   1: Parallel by event type (8 workers)
 *   2: Parallel cursor chains (N independent paginations)
 *   3: Single pipelined cursor (fallback)
 *
 * Optimizations:
 *  - Async insert queue: workers push IDs, background flusher writes to DB
 *  - Parallel strategy combination: stream + cursors run simultaneously
 *  - SSE stream handler for /events/stream
 */

import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";
import {
  apiRequest,
  fetchEvents,
  fetchStream,
  requestStreamAccess,
  pickAuthMethod,
  waitForRateLimit,
  acquireSlot,
  releaseSlot,
  AuthMethod,
  EventsResponse,
  RateLimitInfo,
  StreamAccessResult,
  throughputTracker,
  RateBudgetPacer,
  setMaxInFlight,
  getMaxInFlight,
} from "./api";
import {
  batchInsertIds,
  batchInsertIdsWithRetry,
  batchInsertEvents,
  getIngestedCount,
  getExactCount,
  saveProgress,
  analyzeTable,
} from "./db";
import { quickDiscover, DiscoveryResult } from "./discover";

const TOTAL_EVENTS = 3_000_000;
const PROGRESS_INTERVAL = 10_000;
const ADAPTIVE_INTERVAL = 5000; // check throughput every 5s

// Shared counters
let globalIngested = 0; // DB-confirmed inserts
let globalEnqueued = 0; // IDs pushed to insert queue (leads globalIngested)
let globalStartTime = 0;
let shuttingDown = false;
let cachedDiscovery: DiscoveryResult | null = null;
let adaptiveTimer: ReturnType<typeof setInterval> | null = null;
let activePacer: RateBudgetPacer | null = null;

// =============================================================================
// Adaptive Concurrency Controller
// =============================================================================

function startAdaptiveConcurrency(): void {
  let prevRate = 0;
  adaptiveTimer = setInterval(() => {
    const currentRate = throughputTracker.getRate(undefined, ADAPTIVE_INTERVAL);
    const concurrency = getMaxInFlight();

    if (prevRate > 0) {
      if (currentRate > prevRate * 1.05) {
        // Throughput improving — ramp up
        setMaxInFlight(Math.min(concurrency + 5, 100));
      } else if (currentRate < prevRate * 0.85) {
        // Throughput degrading — back off
        setMaxInFlight(Math.max(concurrency - 5, 5));
      }
    }

    prevRate = currentRate;
  }, ADAPTIVE_INTERVAL);
}

function stopAdaptiveConcurrency(): void {
  if (adaptiveTimer) {
    clearInterval(adaptiveTimer);
    adaptiveTimer = null;
  }
}

// =============================================================================
// Worker Health Watchdog — detects stuck workers and reports health
// =============================================================================

interface WorkerHeartbeat {
  name: string;
  lastProgress: number; // epoch ms of last progress
  fetched: number;
}

const workerHeartbeats: Map<string, WorkerHeartbeat> = new Map();
let healthTimer: ReturnType<typeof setInterval> | null = null;
const HEALTH_INTERVAL = 10_000; // check every 10s
const STUCK_THRESHOLD = 30_000; // warn after 30s no progress
const DEAD_THRESHOLD = 60_000; // consider dead after 60s

function reportWorkerAlive(name: string, fetched: number): void {
  workerHeartbeats.set(name, { name, lastProgress: Date.now(), fetched });
}

function startHealthWatchdog(): void {
  healthTimer = setInterval(() => {
    const now = Date.now();
    let active = 0;
    let stuck = 0;
    let dead = 0;
    const issues: string[] = [];

    for (const [name, hb] of workerHeartbeats) {
      const elapsed = now - hb.lastProgress;
      if (elapsed > DEAD_THRESHOLD) {
        dead++;
        issues.push(`${name}: DEAD (${(elapsed / 1000).toFixed(0)}s silent, fetched=${hb.fetched})`);
      } else if (elapsed > STUCK_THRESHOLD) {
        stuck++;
        issues.push(`${name}: STUCK (${(elapsed / 1000).toFixed(0)}s silent, fetched=${hb.fetched})`);
      } else {
        active++;
      }
    }

    const queueDepth = insertQueue.length;
    const queueLag = globalEnqueued - globalIngested;
    const elapsed = (now - globalStartTime) / 1000;
    const rate = elapsed > 0 ? globalIngested / elapsed : 0;

    console.log(
      `[health] Workers: ${active} active, ${stuck} stuck, ${dead} dead | ` +
      `Queue: ${queueDepth} batches, lag=${queueLag} | ` +
      `Rate: ${rate.toFixed(0)} evt/s | ` +
      `Progress: ${globalIngested.toLocaleString()}/${TOTAL_EVENTS.toLocaleString()} (${((globalIngested / TOTAL_EVENTS) * 100).toFixed(1)}%)`
    );

    if (issues.length > 0) {
      for (const issue of issues) {
        console.warn(`[health] WARNING: ${issue}`);
      }
    }
  }, HEALTH_INTERVAL);
}

function stopHealthWatchdog(): void {
  if (healthTimer) {
    clearInterval(healthTimer);
    healthTimer = null;
  }
}

// =============================================================================
// Event Schema Introspection — log once on first batch to understand the data
// =============================================================================

let schemaLogged = false;

function logEventSchema(events: Array<Record<string, unknown>>, cursor?: string): void {
  if (schemaLogged || events.length === 0) return;
  schemaLogged = true;

  const sample = events[0];
  const fields = Object.keys(sample);

  console.log("[schema] === FIRST BATCH EVENT SCHEMA ===");
  console.log(`[schema]   Events in batch: ${events.length}`);
  console.log(`[schema]   Fields (${fields.length}): ${fields.join(", ")}`);

  for (const field of fields) {
    const val = sample[field];
    const type = val === null ? "null" : Array.isArray(val) ? `array[${(val as unknown[]).length}]` : typeof val;
    let preview: string;
    if (val === null) preview = "null";
    else if (typeof val === "string") preview = `"${val.substring(0, 120)}"`;
    else if (typeof val === "number" || typeof val === "boolean") preview = String(val);
    else preview = JSON.stringify(val).substring(0, 120);
    console.log(`[schema]     ${field}: ${type} = ${preview}`);
  }

  // Detect which ID field works
  const idFields = ["id", "eventId", "_id", "event_id"];
  const usedIdField = idFields.find((f) => sample[f] !== undefined && sample[f] !== null);
  console.log(`[schema]   ID extraction: using "${usedIdField || "NONE"}" → ${usedIdField ? sample[usedIdField] : "NO ID FOUND"}`);

  // Detect timestamp format
  const tsFields = ["timestamp", "created_at", "createdAt", "time", "ts", "occurred_at"];
  const usedTsField = tsFields.find((f) => sample[f] !== undefined && sample[f] !== null);
  if (usedTsField) {
    const tsVal = sample[usedTsField];
    console.log(`[schema]   Timestamp: "${usedTsField}" = ${tsVal}`);
  }

  // Log cursor
  if (cursor) {
    console.log(`[schema]   Cursor value: ${cursor}`);
    console.log(`[schema]   Cursor length: ${cursor.length}`);
  }

  // Check consistency across batch
  if (events.length > 1) {
    const schemas = events.slice(0, 10).map((e) => Object.keys(e).sort().join(","));
    const allSame = schemas.every((s) => s === schemas[0]);
    console.log(`[schema]   Schema consistent across batch: ${allSame ? "YES" : "NO — fields vary!"}`);
    if (!allSame) {
      const unique = [...new Set(schemas)];
      console.log(`[schema]   Unique schemas: ${unique.length}`);
    }
  }

  console.log("[schema] === END SCHEMA ===");
}

function extractId(event: Record<string, unknown>): string {
  return String(event.id || event.eventId || event._id || event.event_id || "");
}

function extractValidIds(events: Array<Record<string, unknown>>): string[] {
  const ids: string[] = [];
  for (const event of events) {
    const id = extractId(event);
    if (id && id !== "undefined" && id !== "null") {
      ids.push(id);
    }
  }
  return ids;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function logProgress(prefix: string, count: number, extra?: string): void {
  const elapsed = (Date.now() - globalStartTime) / 1000;
  const rate = globalIngested / Math.max(elapsed, 1);
  const pct = ((globalIngested / TOTAL_EVENTS) * 100).toFixed(1);
  const eta = rate > 0 ? ((TOTAL_EVENTS - globalIngested) / rate / 60).toFixed(1) : "?";
  const queueLag = globalEnqueued - globalIngested;
  console.log(
    `[${prefix}] ${globalIngested.toLocaleString()}/${TOTAL_EVENTS.toLocaleString()} (${pct}%) | ${rate.toFixed(0)} evt/s | ETA ${eta}m | enq=${globalEnqueued.toLocaleString()} lag=${queueLag}${extra ? " | " + extra : ""}`
  );
}

// =============================================================================
// File-based Write-Ahead Log — never lose a fetched ID
// =============================================================================

const WAL_PATH = path.join(process.env.WAL_DIR || "/tmp", "ingested_ids.wal");
let walStream: fs.WriteStream | null = null;

function getWalStream(): fs.WriteStream {
  if (!walStream) {
    walStream = fs.createWriteStream(WAL_PATH, { flags: "a" }); // append mode
  }
  return walStream;
}

/** Append IDs to WAL file immediately (sync-ish — buffered by OS). */
function walAppend(ids: string[]): void {
  try {
    getWalStream().write(ids.join("\n") + "\n");
  } catch (err) {
    console.error(`[wal] Write error: ${String(err).substring(0, 100)}`);
  }
}

/** On startup, recover any IDs in the WAL file that aren't in the DB yet.
 *  Uses streaming readline to avoid loading the entire WAL into memory. */
export async function recoverFromWal(): Promise<number> {
  if (!fs.existsSync(WAL_PATH)) return 0;

  const stat = fs.statSync(WAL_PATH);
  if (stat.size === 0) return 0;

  console.log(`[wal] Found WAL file (${(stat.size / 1024 / 1024).toFixed(1)} MB). Recovering with streaming reader...`);

  const CHUNK = 50000;
  let recovered = 0;
  let totalLines = 0;
  let batch: string[] = [];

  const rl = readline.createInterface({
    input: fs.createReadStream(WAL_PATH, { encoding: "utf-8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    batch.push(trimmed);
    totalLines++;

    if (batch.length >= CHUNK) {
      const inserted = await batchInsertIdsWithRetry(batch);
      recovered += inserted;
      if (totalLines % (CHUNK * 5) === 0) {
        console.log(`[wal] Recovery progress: ${totalLines.toLocaleString()} lines read, ${recovered.toLocaleString()} inserted`);
      }
      batch = [];
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    const inserted = await batchInsertIdsWithRetry(batch);
    recovered += inserted;
  }

  console.log(`[wal] Recovered ${recovered} missing IDs from ${totalLines} WAL lines`);
  return recovered;
}

export function closeWal(): void {
  if (walStream) {
    walStream.end();
    walStream = null;
  }
}

// =============================================================================
// Async Insert Queue - decouples API fetches from DB writes
// =============================================================================

const insertQueue: string[][] = [];
let insertFlushing = false;
let insertFlushPromise: Promise<void> | null = null;

function enqueueIds(ids: string[], strategy?: string): void {
  if (ids.length > 0) {
    walAppend(ids); // WAL first — survives crashes
    globalEnqueued += ids.length;
    insertQueue.push(ids);
    throughputTracker.record(ids.length, strategy || "unknown");
    if (!insertFlushing) {
      startFlushLoop();
    }
  }
}

function startFlushLoop(): void {
  if (insertFlushing) return;
  insertFlushing = true;
  insertFlushPromise = (async () => {
    while (insertQueue.length > 0 || !shuttingDown) {
      if (insertQueue.length === 0) {
        await sleep(10);
        // Break if queue is empty and no more data expected
        if (insertQueue.length === 0 && shuttingDown) break;
        continue;
      }
      // Drain up to 10 batches at once for efficiency
      const batches: string[][] = [];
      while (insertQueue.length > 0 && batches.length < 10) {
        batches.push(insertQueue.shift()!);
      }
      const combined = batches.flat();
      try {
        const inserted = await batchInsertIdsWithRetry(combined);
        globalIngested += inserted;
      } catch (err) {
        console.error(`[flush] Insert error: ${String(err).substring(0, 200)}`);
        // Re-queue on failure
        insertQueue.unshift(combined);
        await sleep(500);
      }
    }
    insertFlushing = false;
  })();
}

async function drainInsertQueue(): Promise<void> {
  // Process all remaining items
  while (insertQueue.length > 0) {
    const batch = insertQueue.shift()!;
    try {
      const inserted = await batchInsertIdsWithRetry(batch);
      globalIngested += inserted;
    } catch (err) {
      console.error(`[flush] Final drain error: ${String(err).substring(0, 200)}`);
    }
  }
}

// =============================================================================
// Layer -2: /events/all or /events/ids (instant if available)
// =============================================================================

async function ingestAll(endpoint: string): Promise<boolean> {
  console.log(`[ingest] STRATEGY: ${endpoint} (attempting to get all events at once)`);

  try {
    const result = await apiRequest(endpoint, { acceptHeader: "application/json" });
    if (result.statusCode >= 400) {
      console.log(`[ingest] ${endpoint} returned ${result.statusCode}, skipping`);
      return false;
    }

    const contentType = String(result.headers["content-type"] || "");
    console.log(`[ingest] ${endpoint} content-type: ${contentType}, body size: ${result.body.length}`);

    let ids: string[] = [];

    if (contentType.includes("ndjson") || contentType.includes("x-ndjson")) {
      const lines = result.body.split("\n").filter((l) => l.trim());
      for (const line of lines) {
        try {
          const obj = JSON.parse(line);
          const id = typeof obj === "string" ? obj : extractId(obj);
          if (id && id !== "undefined" && id !== "null") ids.push(id);
        } catch {
          // If line looks like a plain ID
          const trimmed = line.trim();
          if (trimmed && trimmed.length > 5 && trimmed.length < 200) ids.push(trimmed);
        }
      }
    } else {
      const parsed = JSON.parse(result.body);
      const data = Array.isArray(parsed) ? parsed : parsed.data || parsed.ids || parsed.events || [];

      if (data.length > 0) {
        if (typeof data[0] === "string") {
          // Array of ID strings
          ids = data.filter((id: string) => id && id !== "undefined" && id !== "null");
        } else {
          // Array of event objects
          ids = extractValidIds(data);
        }
      }
    }

    if (ids.length === 0) {
      console.log(`[ingest] ${endpoint} returned no usable data`);
      return false;
    }

    console.log(`[ingest] ${endpoint} returned ${ids.length} IDs! Inserting...`);

    // Insert in large chunks for speed
    const CHUNK = 50000;
    for (let i = 0; i < ids.length; i += CHUNK) {
      const chunk = ids.slice(i, i + CHUNK);
      const inserted = await batchInsertIdsWithRetry(chunk);
      globalIngested += inserted;
      if (i % (CHUNK * 2) === 0) {
        logProgress("all", globalIngested, `chunk ${i / CHUNK + 1}`);
      }
    }

    await saveProgress(null, globalIngested, "completed", endpoint);
    return true;
  } catch (err) {
    console.log(`[ingest] ${endpoint} error: ${String(err).substring(0, 200)}`);
    return false;
  }
}

// =============================================================================
// Layer -1: Streaming Endpoint (fastest - bypasses rate limits)
// =============================================================================

async function ingestStream(
  streamUrl: string,
  expiresIn: number,
  token?: string | null,
  tokenHeader?: string | null
): Promise<void> {
  console.log(`[ingest] STRATEGY: Stream endpoint -> ${streamUrl} (expires in ${expiresIn}s)`);

  let cursor: string | undefined;
  let consecutiveErrors = 0;
  let consecutiveEmpty = 0;
  const maxRetries = 5;
  let streamDeadline = Date.now() + (expiresIn - 30) * 1000;
  let currentToken = token;
  let currentTokenHeader = tokenHeader;

  while (!shuttingDown && globalEnqueued < TOTAL_EVENTS) {
    // Renew stream access if approaching expiry
    if (Date.now() > streamDeadline) {
      console.log("[stream] Stream token expiring, requesting renewal...");
      const renewed = await requestStreamAccess();
      if (renewed) {
        streamUrl = renewed.endpoint;
        streamDeadline = Date.now() + (renewed.expiresIn - 30) * 1000;
        currentToken = renewed.token;
        currentTokenHeader = renewed.tokenHeader;
        console.log(`[stream] Renewed stream endpoint: ${streamUrl} (expires in ${renewed.expiresIn}s)`);
      } else {
        console.log("[stream] Could not renew stream, falling back to other strategies");
        return ingestFallback();
      }
    }

    try {
      const response = await fetchStream(streamUrl, cursor, undefined, currentToken, currentTokenHeader);

      if (response.statusCode >= 400) {
        consecutiveErrors++;
        console.log(`[stream] Error ${response.statusCode}: ${response.body.substring(0, 200)}`);
        if (consecutiveErrors >= maxRetries) {
          console.log("[stream] Too many errors, falling back");
          return ingestFallback();
        }
        await sleep(Math.min(1000 * Math.pow(2, consecutiveErrors), 30000));
        continue;
      }

      let parsed: any;
      try {
        parsed = JSON.parse(response.body);
      } catch {
        // Could be NDJSON or SSE
        const body = response.body;

        // Check for SSE format (lines starting with "data:")
        if (body.includes("data:")) {
          const ids = parseSSEData(body);
          if (ids.length > 0) {
            enqueueIds(ids, "sse");
            consecutiveErrors = 0;
            consecutiveEmpty = 0;
            if (globalIngested % PROGRESS_INTERVAL < ids.length) {
              logProgress("sse", globalIngested, `batch=${ids.length}`);
            }
          }
          continue;
        }

        // NDJSON
        const lines = body.split("\n").filter((l) => l.trim());
        if (lines.length > 0) {
          const ids: string[] = [];
          for (const line of lines) {
            try {
              const event = JSON.parse(line);
              ids.push(extractId(event));
            } catch {
              // skip malformed
            }
          }
          const validIds = ids.filter((id) => id && id !== "undefined" && id !== "null");
          if (validIds.length > 0) {
            enqueueIds(validIds, "stream-ndjson");
            consecutiveErrors = 0;
            consecutiveEmpty = 0;
            logProgress("stream", globalIngested, `batch=${validIds.length}`);
          }
          continue;
        }
        consecutiveErrors++;
        if (consecutiveErrors >= maxRetries) return ingestFallback();
        await sleep(1000 * consecutiveErrors);
        continue;
      }

      const events: Record<string, unknown>[] = Array.isArray(parsed)
        ? parsed
        : parsed.data || parsed.events || [];

      // Log schema from first batch received via stream
      logEventSchema(events, parsed.nextCursor || parsed.cursor);

      if (events.length === 0) {
        consecutiveEmpty++;
        if (consecutiveEmpty >= 5) {
          console.log("[stream] No more data from stream");
          break;
        }
        await sleep(500);
        continue;
      }

      const ids = extractValidIds(events);
      enqueueIds(ids, "stream");

      cursor = parsed.nextCursor || parsed.cursor;
      consecutiveErrors = 0;
      consecutiveEmpty = 0;

      if (globalIngested % PROGRESS_INTERVAL < events.length) {
        logProgress("stream", globalIngested, `batch=${events.length}`);
      }

      if (globalIngested % 50000 < events.length) {
        await saveProgress(cursor || null, globalIngested, "running", "stream");
      }

      if (!cursor && !parsed.hasMore) {
        console.log("[stream] Stream exhausted (no cursor, no hasMore)");
        break;
      }
    } catch (err) {
      consecutiveErrors++;
      console.error(`[stream] Error (${consecutiveErrors}):`, String(err).substring(0, 200));
      if (consecutiveErrors >= maxRetries) {
        console.log("[stream] Too many errors, falling back");
        return ingestFallback();
      }
      await sleep(Math.min(1000 * Math.pow(2, consecutiveErrors), 30000));
    }
  }

  await drainInsertQueue();
  await analyzeTable();
  const finalCount = await getExactCount();
  globalIngested = finalCount;
  console.log(`[ingest] Stream finished. DB count: ${finalCount}`);
  await saveProgress(null, finalCount, "completed", "stream");
}

// =============================================================================
// SSE Parser - handles Server-Sent Events format
// =============================================================================

function parseSSEData(body: string): string[] {
  const ids: string[] = [];
  const lines = body.split("\n");

  for (const line of lines) {
    if (!line.startsWith("data:")) continue;
    const data = line.substring(5).trim();
    if (!data || data === "[DONE]") continue;

    try {
      const parsed = JSON.parse(data);
      if (typeof parsed === "string") {
        ids.push(parsed);
      } else if (Array.isArray(parsed)) {
        for (const item of parsed) {
          const id = typeof item === "string" ? item : extractId(item);
          if (id && id !== "undefined" && id !== "null") ids.push(id);
        }
      } else if (parsed.id || parsed.eventId) {
        const id = extractId(parsed);
        if (id && id !== "undefined" && id !== "null") ids.push(id);
      }
    } catch {
      // If it's a plain ID string
      if (data.length > 5 && data.length < 200 && !data.includes("{")) {
        ids.push(data);
      }
    }
  }

  return ids;
}

// =============================================================================
// Layer 0: Bulk Endpoint Ingestion
// =============================================================================

async function ingestBulk(endpoint: string): Promise<void> {
  console.log(`[ingest] STRATEGY: Bulk endpoint -> ${endpoint}`);

  const result = await apiRequest(endpoint);
  if (result.statusCode >= 400) {
    console.log(`[ingest] Bulk endpoint returned ${result.statusCode}, falling back`);
    return ingestFallback();
  }

  const contentType = String(result.headers["content-type"] || "");
  console.log(`[ingest] Bulk content-type: ${contentType}, body size: ${result.body.length}`);

  if (contentType.includes("ndjson") || contentType.includes("x-ndjson")) {
    const lines = result.body.split("\n").filter((l) => l.trim());
    console.log(`[ingest] Processing ${lines.length} NDJSON lines`);
    const CHUNK = 5000;
    for (let i = 0; i < lines.length; i += CHUNK) {
      const ids = lines.slice(i, i + CHUNK).map((line) => {
        const event = JSON.parse(line);
        return extractId(event);
      }).filter((id) => id && id !== "undefined" && id !== "null");
      enqueueIds(ids, "bulk");
      if (i % (CHUNK * 10) === 0) logProgress("bulk", globalIngested);
    }
  } else if (contentType.includes("csv")) {
    const lines = result.body.split("\n").filter((l) => l.trim());
    const headers = lines[0].split(",").map((h) => h.trim().replace(/"/g, ""));
    const idIdx = headers.findIndex((h) => h === "id" || h === "eventId" || h === "event_id");
    if (idIdx >= 0) {
      const CHUNK = 5000;
      for (let i = 1; i < lines.length; i += CHUNK) {
        const ids = lines.slice(i, i + CHUNK).map((line) => {
          const vals = line.split(",").map((v) => v.trim().replace(/"/g, ""));
          return vals[idIdx];
        }).filter((id) => id && id !== "undefined" && id !== "null");
        enqueueIds(ids, "bulk-csv");
      }
    }
  } else {
    const parsed = JSON.parse(result.body);
    const events: Record<string, unknown>[] = Array.isArray(parsed) ? parsed : parsed.data || [];
    console.log(`[ingest] Bulk returned ${events.length} events`);
    const CHUNK = 5000;
    for (let i = 0; i < events.length; i += CHUNK) {
      const ids = extractValidIds(events.slice(i, i + CHUNK));
      enqueueIds(ids, "bulk");
      if (i % (CHUNK * 10) === 0) logProgress("bulk", globalIngested);
    }
  }

  await drainInsertQueue();
  await saveProgress(null, globalIngested, "completed", "bulk");
}

// =============================================================================
// Layer 1: Parallel by Event Type (8 workers)
// =============================================================================

async function ingestParallelByType(discovery: DiscoveryResult): Promise<void> {
  const { eventTypes, typeFilterParam, maxLimit } = discovery;
  console.log(`[ingest] STRATEGY: Parallel by type (${eventTypes.length} workers, limit=${maxLimit}, param=${typeFilterParam})`);

  const workers = eventTypes.map((eventType, idx) => {
    const preferredAuth: AuthMethod = idx % 2 === 0 ? "header" : "query";
    return typeWorker(idx, eventType, typeFilterParam, maxLimit, preferredAuth);
  });

  await Promise.all(workers);
  await drainInsertQueue();

  await analyzeTable();
  const finalCount = await getExactCount();
  globalIngested = finalCount;
  console.log(`[ingest] All type workers finished. DB count: ${finalCount}`);
  await saveProgress(null, finalCount, "completed", "parallel-by-type");
}

async function typeWorker(
  id: number,
  eventType: string,
  filterParam: string,
  limit: number,
  preferredAuth: AuthMethod
): Promise<void> {
  let cursor: string | undefined;
  let hasMore = true;
  let fetched = 0;
  let authMethod = preferredAuth;
  let consecutiveErrors = 0;

  console.log(`[worker-${id}] Starting: type=${eventType}, auth=${authMethod}, limit=${limit}`);

  while (hasMore && !shuttingDown) {
    try {
      await waitForRateLimit();
      await acquireSlot();
      let events: EventsResponse;
      let rateLimit: RateLimitInfo;
      let usedAuth: AuthMethod;
      try {
        authMethod = pickAuthMethod(authMethod);
        ({ events, rateLimit, authMethod: usedAuth } = await fetchEvents(
          cursor,
          limit,
          { [filterParam]: eventType },
          authMethod
        ));
      } finally {
        releaseSlot();
      }

      authMethod = usedAuth;

      if (!events.data || events.data.length === 0) {
        console.log(`[worker-${id}] No more data for ${eventType}`);
        break;
      }

      logEventSchema(events.data, events.nextCursor);

      const ids = extractValidIds(events.data);
      enqueueIds(ids, "type-worker");

      hasMore = events.hasMore;
      cursor = events.nextCursor;
      fetched += events.data.length;

      if (rateLimit.remaining <= 2) {
        authMethod = authMethod === "header" ? "query" : "header";
      }

      consecutiveErrors = 0;
      reportWorkerAlive(`type-${id}:${eventType}`, fetched);

      if (fetched % PROGRESS_INTERVAL === 0 || fetched % (limit * 10) < limit) {
        logProgress(`w${id}:${eventType}`, fetched, `cursor=${cursor?.substring(0, 12)}...`);
      }

      if (fetched % 50000 === 0) {
        await saveProgress(cursor || null, globalIngested, "running", "parallel-by-type", {
          workerId: id,
          eventType,
          workerFetched: fetched,
        });
      }
    } catch (err) {
      consecutiveErrors++;
      console.error(`[worker-${id}] Error (${consecutiveErrors}):`, String(err).substring(0, 200));

      if (consecutiveErrors >= 10) {
        console.error(`[worker-${id}] Too many errors, stopping`);
        break;
      }

      if (String(err).includes("cursor") || String(err).includes("expired")) {
        console.log(`[worker-${id}] Cursor expired, restarting from beginning`);
        cursor = undefined;
      }

      await sleep(1000 * consecutiveErrors);
    }
  }

  console.log(`[worker-${id}] Finished ${eventType}: fetched=${fetched}`);
}

// =============================================================================
// Layer 2: Parallel Cursor Chains
// =============================================================================

async function ingestParallelCursors(discovery: DiscoveryResult): Promise<void> {
  const numChains = parseInt(process.env.WORKER_CONCURRENCY || "10", 10);
  const { maxLimit } = discovery;
  console.log(`[ingest] STRATEGY: Parallel cursors (${numChains} chains, limit=${maxLimit})`);

  // Stagger: seed N-1 unique cursors by fetching sequential pages
  // Worker 0 starts from the beginning; workers 1..N-1 start from staggered cursors
  const startCursors: (string | undefined)[] = [undefined]; // worker 0 starts fresh
  console.log(`[ingest] Seeding ${numChains - 1} staggered cursor positions...`);

  let seedCursor: string | undefined;
  for (let i = 1; i < numChains; i++) {
    try {
      const { events } = await fetchEvents(seedCursor, maxLimit, undefined, "header");
      if (events.data && events.data.length > 0) {
        const ids = extractValidIds(events.data);
        enqueueIds(ids, "cursor-seed"); // don't waste the fetched data
      }
      seedCursor = events.nextCursor;
      if (!seedCursor || !events.hasMore) {
        console.log(`[ingest] Seeding exhausted at position ${i}, using ${i} chains`);
        break;
      }
      startCursors.push(seedCursor);
    } catch (err) {
      console.log(`[ingest] Seeding error at position ${i}: ${String(err).substring(0, 100)}`);
      break;
    }
  }

  console.log(`[ingest] Starting ${startCursors.length} staggered cursor chains`);

  const workers = startCursors.map((cursor, idx) => {
    const authMethod: AuthMethod = idx % 2 === 0 ? "header" : "query";
    return cursorChainWorker(idx, maxLimit, authMethod, cursor);
  });

  await Promise.all(workers);
  await drainInsertQueue();

  await analyzeTable();
  const finalCount = await getExactCount();
  globalIngested = finalCount;
  console.log(`[ingest] All cursor chains finished. DB count: ${finalCount}`);
  await saveProgress(null, finalCount, "completed", "parallel-cursors");
}

async function cursorChainWorker(
  id: number,
  limit: number,
  preferredAuth: AuthMethod,
  startCursor?: string
): Promise<void> {
  let cursor: string | undefined = startCursor;
  let hasMore = true;
  let fetched = 0;
  let authMethod = preferredAuth;
  let consecutiveErrors = 0;

  while (hasMore && !shuttingDown && globalEnqueued < TOTAL_EVENTS) {
    try {
      await waitForRateLimit();
      await acquireSlot();
      let events: EventsResponse;
      let rateLimit: RateLimitInfo;
      let usedAuth: AuthMethod;
      try {
        authMethod = pickAuthMethod(authMethod);
        ({ events, rateLimit, authMethod: usedAuth } = await fetchEvents(cursor, limit, undefined, authMethod));
      } finally {
        releaseSlot();
      }
      authMethod = usedAuth;

      if (!events.data || events.data.length === 0) break;

      logEventSchema(events.data, events.nextCursor);

      const ids = extractValidIds(events.data);
      enqueueIds(ids, "cursor-chain");

      hasMore = events.hasMore;
      cursor = events.nextCursor;
      fetched += events.data.length;

      if (rateLimit.remaining <= 2) {
        authMethod = authMethod === "header" ? "query" : "header";
      }

      consecutiveErrors = 0;
      reportWorkerAlive(`chain-${id}`, fetched);

      if (fetched % PROGRESS_INTERVAL === 0) {
        logProgress(`chain-${id}`, fetched);
      }
    } catch (err) {
      consecutiveErrors++;
      if (consecutiveErrors >= 10) break;
      if (String(err).includes("cursor") || String(err).includes("expired")) {
        cursor = undefined;
      }
      await sleep(1000 * consecutiveErrors);
    }
  }

  console.log(`[chain-${id}] Finished: fetched=${fetched}`);
}

// =============================================================================
// Layer 2.5: Parallel Offset-based Pagination (cleanest parallelism, no overlap)
// =============================================================================

async function ingestParallelOffset(discovery: DiscoveryResult): Promise<void> {
  const numWorkers = parseInt(process.env.WORKER_CONCURRENCY || "10", 10);
  const { maxLimit, offsetParam } = discovery;
  const rangeSize = Math.ceil(TOTAL_EVENTS / numWorkers);
  console.log(`[ingest] STRATEGY: Parallel offset (${numWorkers} workers, limit=${maxLimit}, param=${offsetParam}, range=${rangeSize})`);

  const workers = Array.from({ length: numWorkers }, (_, idx) => {
    const startOffset = idx * rangeSize;
    const endOffset = Math.min(startOffset + rangeSize, TOTAL_EVENTS);
    const authMethod: AuthMethod = idx % 2 === 0 ? "header" : "query";
    return offsetWorker(idx, startOffset, endOffset, maxLimit, offsetParam, authMethod);
  });

  await Promise.all(workers);
  await drainInsertQueue();

  await analyzeTable();
  const finalCount = await getExactCount();
  globalIngested = finalCount;
  console.log(`[ingest] All offset workers finished. DB count: ${finalCount}`);
  await saveProgress(null, finalCount, "completed", "parallel-offset");
}

async function offsetWorker(
  id: number,
  startOffset: number,
  endOffset: number,
  limit: number,
  offsetParam: string,
  preferredAuth: AuthMethod
): Promise<void> {
  let currentOffset = startOffset;
  let fetched = 0;
  let authMethod = preferredAuth;
  let consecutiveErrors = 0;

  console.log(`[offset-${id}] Starting: range=[${startOffset}, ${endOffset}), auth=${authMethod}`);

  while (currentOffset < endOffset && !shuttingDown && globalEnqueued < TOTAL_EVENTS) {
    try {
      await waitForRateLimit();
      await acquireSlot();
      let events: EventsResponse;
      let rateLimit: RateLimitInfo;
      let usedAuth: AuthMethod;
      try {
        authMethod = pickAuthMethod(authMethod);
        ({ events, rateLimit, authMethod: usedAuth } = await fetchEvents(
          undefined,
          limit,
          { [offsetParam]: String(currentOffset) },
          authMethod
        ));
      } finally {
        releaseSlot();
      }
      authMethod = usedAuth;

      if (!events.data || events.data.length === 0) {
        console.log(`[offset-${id}] No data at offset ${currentOffset}`);
        break;
      }

      const ids = extractValidIds(events.data);
      enqueueIds(ids, "offset");

      currentOffset += events.data.length;
      fetched += events.data.length;

      if (rateLimit.remaining <= 2) {
        authMethod = authMethod === "header" ? "query" : "header";
      }

      consecutiveErrors = 0;
      reportWorkerAlive(`offset-${id}`, fetched);

      if (fetched % PROGRESS_INTERVAL === 0) {
        logProgress(`offset-${id}`, fetched, `at=${currentOffset}`);
      }
    } catch (err) {
      consecutiveErrors++;
      if (consecutiveErrors >= 10) break;
      await sleep(1000 * consecutiveErrors);
    }
  }

  console.log(`[offset-${id}] Finished: fetched=${fetched}`);
}

// =============================================================================
// Layer 3: Single Pipelined Cursor (fallback)
// =============================================================================

async function ingestPipelined(discovery: DiscoveryResult): Promise<void> {
  const { maxLimit } = discovery;
  console.log(`[ingest] STRATEGY: Single pipelined cursor (limit=${maxLimit})`);

  let cursor: string | undefined;
  let hasMore = true;
  let fetched = 0;
  let authMethod: AuthMethod = "header";
  let consecutiveErrors = 0;

  while (hasMore && !shuttingDown && globalEnqueued < TOTAL_EVENTS) {
    try {
      await waitForRateLimit();
      await acquireSlot();
      let events: EventsResponse;
      let rateLimit: RateLimitInfo;
      let usedAuth: AuthMethod;
      try {
        authMethod = pickAuthMethod(authMethod);
        ({ events, rateLimit, authMethod: usedAuth } = await fetchEvents(cursor, maxLimit, undefined, authMethod));
      } finally {
        releaseSlot();
      }
      authMethod = usedAuth;

      if (!events.data || events.data.length === 0) break;

      const ids = extractValidIds(events.data);
      enqueueIds(ids, "pipelined");

      hasMore = events.hasMore;
      cursor = events.nextCursor;
      fetched += events.data.length;

      if (rateLimit.remaining <= 2) {
        authMethod = authMethod === "header" ? "query" : "header";
      }

      consecutiveErrors = 0;
      reportWorkerAlive("pipelined", fetched);

      if (fetched % PROGRESS_INTERVAL === 0) {
        logProgress("pipe", fetched);
      }

      if (fetched % 50000 === 0) {
        await saveProgress(cursor || null, globalIngested, "running", "pipelined");
      }
    } catch (err) {
      consecutiveErrors++;
      if (consecutiveErrors >= 10) break;
      if (String(err).includes("cursor") || String(err).includes("expired")) {
        cursor = undefined;
      }
      await sleep(1000 * consecutiveErrors);
    }
  }

  await drainInsertQueue();
  const finalCount = await getExactCount();
  globalIngested = finalCount;
  console.log(`[ingest] Pipelined finished. DB count: ${finalCount}`);
  await saveProgress(null, finalCount, "completed", "pipelined");
}

// =============================================================================
// Parallel Strategy Combination: stream + cursor chains simultaneously
// =============================================================================

async function ingestParallelCombo(discovery: DiscoveryResult): Promise<void> {
  console.log("[ingest] STRATEGY: Parallel combo (stream + reduced parallel workers simultaneously)");

  const strategies: Promise<void>[] = [];

  // Stream endpoint has its own rate limit pool — always run it
  if (discovery.streamEndpoint) {
    strategies.push(
      ingestStream(
        discovery.streamEndpoint,
        discovery.streamExpiresIn,
        discovery.streamToken,
        discovery.streamTokenHeader
      )
    );
  }

  // When stream is active, halve the parallel worker count to avoid wasting rate limit quota on duplicates
  const originalConcurrency = process.env.WORKER_CONCURRENCY;
  if (discovery.streamEndpoint) {
    const currentConcurrency = parseInt(process.env.WORKER_CONCURRENCY || "10", 10);
    const reduced = Math.max(Math.floor(currentConcurrency / 2), 2);
    process.env.WORKER_CONCURRENCY = String(reduced);
    console.log(`[ingest] Reduced parallel workers from ${currentConcurrency} to ${reduced} (stream active)`);
  }

  // Parallel workers blast the paginated API simultaneously
  if (discovery.typeFilterWorks) {
    strategies.push(ingestParallelByType(discovery));
  } else {
    strategies.push(ingestParallelCursors(discovery));
  }

  // ON CONFLICT DO NOTHING handles dedup from overlapping workers
  await Promise.all(strategies);

  // Restore original concurrency
  if (originalConcurrency !== undefined) {
    process.env.WORKER_CONCURRENCY = originalConcurrency;
  } else {
    delete process.env.WORKER_CONCURRENCY;
  }

  await drainInsertQueue();
  await analyzeTable();
  const finalCount = await getExactCount();
  globalIngested = finalCount;
  console.log(`[ingest] Parallel combo finished. DB count: ${finalCount}`);
}

// =============================================================================
// Fallback (if primary strategy fails)
// =============================================================================

async function ingestFallback(): Promise<void> {
  const discovery = cachedDiscovery || await quickDiscover();
  cachedDiscovery = discovery;
  if (discovery.offsetWorks) return ingestParallelOffset(discovery);
  if (discovery.typeFilterWorks) return ingestParallelByType(discovery);
  if (discovery.parallelCursorsWork) return ingestParallelCursors(discovery);
  return ingestPipelined(discovery);
}

// =============================================================================
// Orchestrator
// =============================================================================

export async function runIngestion(): Promise<void> {
  globalStartTime = Date.now();
  globalIngested = await getIngestedCount();
  globalEnqueued = globalIngested; // Initialize enqueued from DB count on resume

  if (globalIngested >= TOTAL_EVENTS) {
    console.log(`[ingest] Already have ${globalIngested} events. Skipping.`);
    return;
  }

  if (globalIngested > 0) {
    console.log(`[ingest] Resuming from ${globalIngested} existing events`);
  }

  // Phase 0: Recover any IDs from WAL file that didn't make it to DB
  const walRecovered = await recoverFromWal();
  if (walRecovered > 0) {
    globalIngested = await getExactCount();
    globalEnqueued = globalIngested;
    console.log(`[ingest] After WAL recovery: ${globalIngested} events in DB`);
    if (globalIngested >= TOTAL_EVENTS) {
      console.log("[ingest] WAL recovery completed the job!");
      return;
    }
  }

  // Phase 1: Quick discovery — probe API to find fastest endpoints
  console.log("[ingest] Phase 1: Quick discovery...");
  const discovery = await quickDiscover();
  cachedDiscovery = discovery;

  // Log discovery summary so we can see what was found
  console.log("=== DISCOVERY SUMMARY ===");
  console.log(`  Stream endpoint:   ${discovery.streamEndpoint || "not found"}`);
  console.log(`  Stream token:      ${discovery.streamToken ? "yes" : "no"}`);
  console.log(`  Bulk endpoint:     ${discovery.bulkEndpoint || "not found"}`);
  console.log(`  /events/all:       ${discovery.allEndpoint || "not found"}`);
  console.log(`  /events/ids:       ${discovery.idsEndpoint || "not found"}`);
  console.log(`  Type filter:       ${discovery.typeFilterWorks ? `YES (param: ${discovery.typeFilterParam})` : "no"}`);
  console.log(`  Max page size:     ${discovery.maxLimit}`);
  console.log(`  Offset pagination: ${discovery.offsetWorks ? `YES (param: ${discovery.offsetParam})` : "no"}`);
  console.log(`  Time range:        ${discovery.timeRange ? `${discovery.timeRange.min} → ${discovery.timeRange.max}` : "not found"}`);
  console.log(`  Parallel cursors:  ${discovery.parallelCursorsWork ? "yes" : "unknown"}`);
  console.log(`  Dual rate pools:   ${discovery.dualRateLimitPools ? "YES" : "no"}`);
  console.log(`  Event types:       ${discovery.eventTypes.join(", ")}`);

  // Phase 1.5: Try instant fast-path endpoints (10s timeout each)
  let fastPathDone = false;
  if (discovery.idsEndpoint) {
    console.log("[ingest] Trying fast-path: /events/ids ...");
    fastPathDone = await ingestAll(discovery.idsEndpoint);
  }
  if (!fastPathDone && discovery.allEndpoint) {
    console.log("[ingest] Trying fast-path: /events/all ...");
    fastPathDone = await ingestAll(discovery.allEndpoint);
  }

  if (fastPathDone) {
    const count = await getExactCount();
    globalIngested = count;
    if (count >= TOTAL_EVENTS) {
      console.log(`[ingest] Fast-path got all ${count} events!`);
      // Skip to verification phase
    } else {
      console.log(`[ingest] Fast-path got ${count} events, need more. Continuing...`);
      fastPathDone = false;
    }
  }

  if (!fastPathDone) {
    // Pick strategy
    let strategyName: string;
    if (discovery.offsetWorks) {
      strategyName = `parallel offset (param=${discovery.offsetParam}, limit=${discovery.maxLimit})`;
    } else if (discovery.streamEndpoint) {
      strategyName = "stream + parallel workers";
    } else if (discovery.typeFilterWorks) {
      strategyName = `parallel by type (${discovery.eventTypes.length} workers, limit=${discovery.maxLimit})`;
    } else {
      strategyName = `parallel cursor chains (limit=${discovery.maxLimit})`;
    }
    console.log(`  CHOSEN STRATEGY:   ${strategyName}`);
    console.log(`  SSE works:         ${discovery.sseWorks ? "YES" : "no"}`);
    console.log(`  Time param:        ${discovery.timeParamWorks ? `YES (${discovery.timeParam})` : "no"}`);
    console.log(`  Custom headers:    ${discovery.customHeaders ? JSON.stringify(discovery.customHeaders) : "none"}`);
    console.log(`  Rate budget:       ${discovery.rateLimitBudget ? `H:${discovery.rateLimitBudget.headerLimit}/${discovery.rateLimitBudget.headerWindow}s Q:${discovery.rateLimitBudget.queryLimit}/${discovery.rateLimitBudget.queryWindow}s` : "not measured"}`);
    console.log("=========================");

    // Initialize rate budget pacer if we have measurements
    if (discovery.rateLimitBudget) {
      activePacer = new RateBudgetPacer(discovery.rateLimitBudget);
      console.log(`[ingest] Rate budget pacer initialized: ${activePacer.getTotalRPS().toFixed(1)} RPS`);
    }

    // Start adaptive concurrency controller and health watchdog
    startAdaptiveConcurrency();
    startHealthWatchdog();

    // Phase 2: Execute — parallel workers are the primary path.
    console.log("[ingest] Phase 2: Executing strategy...");

    if (discovery.offsetWorks) {
      await ingestParallelOffset(discovery);
    } else if (discovery.streamEndpoint) {
      await ingestParallelCombo(discovery);
    } else if (discovery.typeFilterWorks) {
      await ingestParallelByType(discovery);
    } else {
      await ingestParallelCursors(discovery);
    }

    // Stop adaptive concurrency and health watchdog
    stopAdaptiveConcurrency();
    stopHealthWatchdog();
  }

  // Phase 3: Verify and retry if shortfall
  await analyzeTable();
  let finalCount = await getExactCount();
  globalIngested = finalCount;

  const MAX_RETRIES = 3;
  let retryNum = 0;
  while (finalCount < TOTAL_EVENTS && retryNum < MAX_RETRIES && !shuttingDown) {
    retryNum++;
    console.log(`[ingest] Shortfall: ${finalCount}/${TOTAL_EVENTS}. Retry ${retryNum}/${MAX_RETRIES} with fallback strategy...`);
    await sleep(2000);

    await ingestFallback();

    await analyzeTable();
    finalCount = await getExactCount();
    globalIngested = finalCount;
    console.log(`[ingest] After retry ${retryNum}: ${finalCount}/${TOTAL_EVENTS}`);
  }

  const elapsed = (Date.now() - globalStartTime) / 1000;
  const rate = finalCount / Math.max(elapsed, 1);

  // Log throughput per strategy
  const bestStrategy = throughputTracker.getBestStrategy();
  if (bestStrategy) {
    console.log(`[ingest] Best strategy by throughput: ${bestStrategy} (${throughputTracker.getRate(bestStrategy).toFixed(0)} evt/s)`);
  }

  console.log(`[ingest] DONE. Final count: ${finalCount} | Time: ${(elapsed / 60).toFixed(1)}m | Avg: ${rate.toFixed(0)} evt/s`);

  if (finalCount < TOTAL_EVENTS) {
    console.log(`[ingest] WARNING: Only ${finalCount}/${TOTAL_EVENTS} events after ${MAX_RETRIES} retries.`);
  }
}

/**
 * Signal handler for graceful shutdown.
 */
export function requestShutdown(): void {
  shuttingDown = true;
  console.log("[ingest] Shutdown requested, finishing current batches...");
}
