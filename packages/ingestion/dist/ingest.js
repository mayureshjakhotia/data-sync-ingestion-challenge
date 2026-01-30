"use strict";
/**
 * Aggressive parallel ingestion engine.
 *
 * Strategy layers (auto-detected, fastest first):
 *   0: Bulk/internal endpoint (if discovered)
 *   1: Parallel by event type (8 workers, each filtering by type)
 *   2: Parallel cursor chains (N independent paginations)
 *   3: Single pipelined cursor (fallback)
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.runIngestion = runIngestion;
exports.requestShutdown = requestShutdown;
const api_1 = require("./api");
const db_1 = require("./db");
const discover_1 = require("./discover");
const TOTAL_EVENTS = 3_000_000;
const PROGRESS_INTERVAL = 10_000; // Log progress every N events per worker
// Shared counter for total ingested (approximate - DB is source of truth)
let globalIngested = 0;
let globalStartTime = 0;
let shuttingDown = false;
function extractId(event) {
    return String(event.id || event.eventId || event._id || event.event_id || "");
}
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
function logProgress(prefix, count, extra) {
    const elapsed = (Date.now() - globalStartTime) / 1000;
    const rate = globalIngested / Math.max(elapsed, 1);
    const pct = ((globalIngested / TOTAL_EVENTS) * 100).toFixed(1);
    const eta = rate > 0 ? ((TOTAL_EVENTS - globalIngested) / rate / 60).toFixed(1) : "?";
    console.log(`[${prefix}] ${globalIngested.toLocaleString()}/${TOTAL_EVENTS.toLocaleString()} (${pct}%) | ${rate.toFixed(0)} evt/s | ETA ${eta}m${extra ? " | " + extra : ""}`);
}
// =============================================================================
// Layer 0: Bulk Endpoint Ingestion
// =============================================================================
async function ingestBulk(endpoint) {
    console.log(`[ingest] STRATEGY: Bulk endpoint -> ${endpoint}`);
    const result = await (0, api_1.apiRequest)(endpoint);
    if (result.statusCode >= 400) {
        console.log(`[ingest] Bulk endpoint returned ${result.statusCode}, falling back`);
        return ingestFallback();
    }
    const contentType = String(result.headers["content-type"] || "");
    console.log(`[ingest] Bulk content-type: ${contentType}, body size: ${result.body.length}`);
    if (contentType.includes("ndjson") || contentType.includes("x-ndjson")) {
        const lines = result.body.split("\n").filter((l) => l.trim());
        console.log(`[ingest] Processing ${lines.length} NDJSON lines`);
        const CHUNK = 2000;
        for (let i = 0; i < lines.length; i += CHUNK) {
            const ids = lines.slice(i, i + CHUNK).map((line) => {
                const event = JSON.parse(line);
                return extractId(event);
            });
            const inserted = await (0, db_1.batchInsertIds)(ids);
            globalIngested += inserted;
            if (i % (CHUNK * 10) === 0)
                logProgress("bulk", globalIngested);
        }
    }
    else if (contentType.includes("csv")) {
        const lines = result.body.split("\n").filter((l) => l.trim());
        const headers = lines[0].split(",").map((h) => h.trim().replace(/"/g, ""));
        const idIdx = headers.findIndex((h) => h === "id" || h === "eventId" || h === "event_id");
        if (idIdx >= 0) {
            const CHUNK = 2000;
            for (let i = 1; i < lines.length; i += CHUNK) {
                const ids = lines.slice(i, i + CHUNK).map((line) => {
                    const vals = line.split(",").map((v) => v.trim().replace(/"/g, ""));
                    return vals[idIdx];
                });
                const inserted = await (0, db_1.batchInsertIds)(ids);
                globalIngested += inserted;
            }
        }
    }
    else {
        // JSON
        const parsed = JSON.parse(result.body);
        const events = Array.isArray(parsed) ? parsed : parsed.data || [];
        console.log(`[ingest] Bulk returned ${events.length} events`);
        const CHUNK = 2000;
        for (let i = 0; i < events.length; i += CHUNK) {
            const ids = events.slice(i, i + CHUNK).map(extractId);
            const inserted = await (0, db_1.batchInsertIds)(ids);
            globalIngested += inserted;
            if (i % (CHUNK * 10) === 0)
                logProgress("bulk", globalIngested);
        }
    }
    await (0, db_1.saveProgress)(null, globalIngested, "completed", "bulk");
}
// =============================================================================
// Layer 1: Parallel by Event Type (8 workers)
// =============================================================================
async function ingestParallelByType(discovery) {
    const { eventTypes, typeFilterParam, maxLimit } = discovery;
    console.log(`[ingest] STRATEGY: Parallel by type (${eventTypes.length} workers, limit=${maxLimit}, param=${typeFilterParam})`);
    const workers = eventTypes.map((eventType, idx) => {
        // Alternate auth methods across workers for 2x rate capacity
        const preferredAuth = idx % 2 === 0 ? "header" : "query";
        return typeWorker(idx, eventType, typeFilterParam, maxLimit, preferredAuth);
    });
    await Promise.all(workers);
    // Verify final count
    await (0, db_1.analyzeTable)();
    const finalCount = await (0, db_1.getExactCount)();
    globalIngested = finalCount;
    console.log(`[ingest] All type workers finished. DB count: ${finalCount}`);
    await (0, db_1.saveProgress)(null, finalCount, "completed", "parallel-by-type");
}
async function typeWorker(id, eventType, filterParam, limit, preferredAuth) {
    let cursor;
    let hasMore = true;
    let fetched = 0;
    let authMethod = preferredAuth;
    let consecutiveErrors = 0;
    console.log(`[worker-${id}] Starting: type=${eventType}, auth=${authMethod}, limit=${limit}`);
    while (hasMore && !shuttingDown) {
        try {
            await (0, api_1.waitForRateLimit)();
            authMethod = (0, api_1.pickAuthMethod)(authMethod);
            const { events, rateLimit, authMethod: usedAuth } = await (0, api_1.fetchEvents)(cursor, limit, { [filterParam]: eventType }, authMethod);
            authMethod = usedAuth;
            if (!events.data || events.data.length === 0) {
                console.log(`[worker-${id}] No more data for ${eventType}`);
                break;
            }
            // Extract IDs and insert
            const ids = events.data.map(extractId);
            const insertPromise = (0, db_1.batchInsertIds)(ids);
            hasMore = events.hasMore;
            cursor = events.nextCursor;
            fetched += events.data.length;
            // Handle rate limits - alternate auth
            if (rateLimit.remaining <= 2) {
                authMethod = authMethod === "header" ? "query" : "header";
            }
            const inserted = await insertPromise;
            globalIngested += inserted;
            consecutiveErrors = 0;
            if (fetched % PROGRESS_INTERVAL === 0 || fetched % (limit * 10) < limit) {
                logProgress(`w${id}:${eventType}`, fetched, `cursor=${cursor?.substring(0, 12)}...`);
            }
            // Save progress periodically
            if (fetched % 50000 === 0) {
                await (0, db_1.saveProgress)(cursor || null, globalIngested, "running", "parallel-by-type", {
                    workerId: id,
                    eventType,
                    workerFetched: fetched,
                });
            }
        }
        catch (err) {
            consecutiveErrors++;
            console.error(`[worker-${id}] Error (${consecutiveErrors}):`, String(err).substring(0, 200));
            if (consecutiveErrors >= 10) {
                console.error(`[worker-${id}] Too many errors, stopping`);
                break;
            }
            // Cursor may have expired - restart from beginning with no cursor
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
async function ingestParallelCursors(discovery) {
    const numChains = 5;
    const { maxLimit } = discovery;
    console.log(`[ingest] STRATEGY: Parallel cursors (${numChains} chains, limit=${maxLimit})`);
    const workers = Array.from({ length: numChains }, (_, idx) => {
        const authMethod = idx % 2 === 0 ? "header" : "query";
        return cursorChainWorker(idx, maxLimit, authMethod);
    });
    await Promise.all(workers);
    await (0, db_1.analyzeTable)();
    const finalCount = await (0, db_1.getExactCount)();
    globalIngested = finalCount;
    console.log(`[ingest] All cursor chains finished. DB count: ${finalCount}`);
    await (0, db_1.saveProgress)(null, finalCount, "completed", "parallel-cursors");
}
async function cursorChainWorker(id, limit, preferredAuth) {
    let cursor;
    let hasMore = true;
    let fetched = 0;
    let authMethod = preferredAuth;
    let consecutiveErrors = 0;
    while (hasMore && !shuttingDown && globalIngested < TOTAL_EVENTS) {
        try {
            await (0, api_1.waitForRateLimit)();
            authMethod = (0, api_1.pickAuthMethod)(authMethod);
            const { events, rateLimit, authMethod: usedAuth } = await (0, api_1.fetchEvents)(cursor, limit, undefined, authMethod);
            authMethod = usedAuth;
            if (!events.data || events.data.length === 0)
                break;
            const ids = events.data.map(extractId);
            const insertPromise = (0, db_1.batchInsertIds)(ids);
            hasMore = events.hasMore;
            cursor = events.nextCursor;
            fetched += events.data.length;
            if (rateLimit.remaining <= 2) {
                authMethod = authMethod === "header" ? "query" : "header";
            }
            const inserted = await insertPromise;
            globalIngested += inserted;
            consecutiveErrors = 0;
            if (fetched % PROGRESS_INTERVAL === 0) {
                logProgress(`chain-${id}`, fetched);
            }
        }
        catch (err) {
            consecutiveErrors++;
            if (consecutiveErrors >= 10)
                break;
            if (String(err).includes("cursor") || String(err).includes("expired")) {
                cursor = undefined;
            }
            await sleep(1000 * consecutiveErrors);
        }
    }
    console.log(`[chain-${id}] Finished: fetched=${fetched}`);
}
// =============================================================================
// Layer 3: Single Pipelined Cursor (fallback)
// =============================================================================
async function ingestPipelined(discovery) {
    const { maxLimit } = discovery;
    console.log(`[ingest] STRATEGY: Single pipelined cursor (limit=${maxLimit})`);
    let cursor;
    let hasMore = true;
    let fetched = 0;
    let authMethod = "header";
    let consecutiveErrors = 0;
    while (hasMore && !shuttingDown && globalIngested < TOTAL_EVENTS) {
        try {
            await (0, api_1.waitForRateLimit)();
            authMethod = (0, api_1.pickAuthMethod)(authMethod);
            // Pipeline: fetch next page while inserting current
            const { events, rateLimit, authMethod: usedAuth } = await (0, api_1.fetchEvents)(cursor, maxLimit, undefined, authMethod);
            authMethod = usedAuth;
            if (!events.data || events.data.length === 0)
                break;
            const ids = events.data.map(extractId);
            const insertPromise = (0, db_1.batchInsertIds)(ids);
            hasMore = events.hasMore;
            cursor = events.nextCursor;
            fetched += events.data.length;
            if (rateLimit.remaining <= 2) {
                authMethod = authMethod === "header" ? "query" : "header";
            }
            const inserted = await insertPromise;
            globalIngested += inserted;
            consecutiveErrors = 0;
            if (fetched % PROGRESS_INTERVAL === 0) {
                logProgress("pipe", fetched);
            }
            if (fetched % 50000 === 0) {
                await (0, db_1.saveProgress)(cursor || null, globalIngested, "running", "pipelined");
            }
        }
        catch (err) {
            consecutiveErrors++;
            if (consecutiveErrors >= 10)
                break;
            if (String(err).includes("cursor") || String(err).includes("expired")) {
                cursor = undefined;
            }
            await sleep(1000 * consecutiveErrors);
        }
    }
    const finalCount = await (0, db_1.getExactCount)();
    globalIngested = finalCount;
    console.log(`[ingest] Pipelined finished. DB count: ${finalCount}`);
    await (0, db_1.saveProgress)(null, finalCount, "completed", "pipelined");
}
// =============================================================================
// Fallback (if bulk fails)
// =============================================================================
async function ingestFallback() {
    const discovery = await (0, discover_1.quickDiscover)();
    if (discovery.typeFilterWorks)
        return ingestParallelByType(discovery);
    if (discovery.parallelCursorsWork)
        return ingestParallelCursors(discovery);
    return ingestPipelined(discovery);
}
// =============================================================================
// Orchestrator
// =============================================================================
async function runIngestion() {
    globalStartTime = Date.now();
    globalIngested = await (0, db_1.getIngestedCount)();
    if (globalIngested >= TOTAL_EVENTS) {
        console.log(`[ingest] Already have ${globalIngested} events. Skipping.`);
        return;
    }
    if (globalIngested > 0) {
        console.log(`[ingest] Resuming from ${globalIngested} existing events`);
    }
    // Phase 1: Quick discovery
    console.log("[ingest] Phase 1: Quick discovery...");
    const discovery = await (0, discover_1.quickDiscover)();
    // Phase 2: Select and run strategy
    console.log("[ingest] Phase 2: Executing strategy...");
    if (discovery.bulkEndpoint) {
        await ingestBulk(discovery.bulkEndpoint);
    }
    else if (discovery.typeFilterWorks) {
        await ingestParallelByType(discovery);
    }
    else if (discovery.parallelCursorsWork) {
        await ingestParallelCursors(discovery);
    }
    else {
        await ingestPipelined(discovery);
    }
    // Phase 3: Verify
    await (0, db_1.analyzeTable)();
    const finalCount = await (0, db_1.getExactCount)();
    const elapsed = (Date.now() - globalStartTime) / 1000;
    const rate = finalCount / Math.max(elapsed, 1);
    console.log(`[ingest] DONE. Final count: ${finalCount} | Time: ${(elapsed / 60).toFixed(1)}m | Avg: ${rate.toFixed(0)} evt/s`);
    if (finalCount < TOTAL_EVENTS) {
        console.log(`[ingest] WARNING: Only ${finalCount}/${TOTAL_EVENTS} events. May need additional passes.`);
    }
}
/**
 * Signal handler for graceful shutdown.
 */
function requestShutdown() {
    shuttingDown = true;
    console.log("[ingest] Shutdown requested, finishing current batches...");
}
