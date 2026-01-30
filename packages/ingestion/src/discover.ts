/**
 * Quick runtime discovery probe (parallel API calls).
 * Returns structured result for strategy selection in ingest.ts.
 */

import { request as undiciRequest } from "undici";
import { probeEndpoint, fetchEvents, apiRequest, getHostBaseUrl, getApiKey, requestStreamAccess, AuthMethod, StreamAccessResult } from "./api";

export interface RateLimitBudget {
  headerLimit: number;
  headerWindow: number; // seconds
  headerRemaining: number;
  queryLimit: number;
  queryWindow: number;
  queryRemaining: number;
  separatePools: boolean;
}

export interface DiscoveryResult {
  maxLimit: number;
  typeFilterWorks: boolean;
  typeFilterParam: string; // "type" or "eventType"
  bulkEndpoint: string | null;
  parallelCursorsWork: boolean;
  eventTypes: string[];
  streamEndpoint: string | null; // from /internal/dashboard/stream-access
  streamExpiresIn: number;
  streamToken: string | null;
  streamTokenHeader: string | null;
  allEndpoint: string | null; // /events/all if it returns data
  idsEndpoint: string | null; // /events/ids if it returns IDs
  batchEndpoint: string | null; // /events/batch if it supports offset
  dualRateLimitPools: boolean; // whether header and query auth have separate pools
  offsetWorks: boolean; // whether offset/page/skip pagination works
  offsetParam: string; // "offset", "page", or "skip"
  timeRange: { min: string; max: string } | null; // earliest/latest event timestamps
  rateLimitBudget: RateLimitBudget | null; // measured rate limit budget
  sseWorks: boolean; // whether /events/sse returns SSE data
  timeParamWorks: boolean; // whether time-based filtering works
  timeParam: string; // "since", "from", etc.
  customHeaders: Record<string, string> | null; // any custom headers that unlock extra throughput
}

// Event types from /internal/stats (confirmed from actual API response)
const EVENT_TYPES = [
  "page_view", "click", "api_call", "form_submit",
  "scroll", "purchase", "error", "video_play",
];

/**
 * Phase 0 deep inspection: fetch a small batch and log EVERYTHING about the response.
 * This is the foundation — all other probes and strategies depend on understanding
 * what the API actually returns.
 */
async function probeApiShape(): Promise<void> {
  console.log("[discover] === DEEP API INSPECTION ===");

  try {
    const result = await apiRequest("/events?limit=5");

    // 1. Log ALL response headers
    console.log("[discover] --- Response Headers (ALL) ---");
    for (const [key, value] of Object.entries(result.headers)) {
      const v = Array.isArray(value) ? value.join(", ") : value;
      console.log(`[discover]   ${key}: ${v}`);
    }

    // 2. Log rate limit headers specifically — detect actual header names
    const rateLimitHeaders: string[] = [];
    for (const key of Object.keys(result.headers)) {
      if (key.toLowerCase().includes("rate") || key.toLowerCase().includes("limit") ||
          key.toLowerCase().includes("retry") || key.toLowerCase().includes("throttl")) {
        rateLimitHeaders.push(key);
      }
    }
    console.log(`[discover] Rate-limit related headers found: ${rateLimitHeaders.length > 0 ? rateLimitHeaders.join(", ") : "NONE"}`);

    // 3. Parse response body and inspect top-level structure
    console.log("[discover] --- Response Body Shape ---");
    console.log(`[discover]   Status: ${result.statusCode}`);
    console.log(`[discover]   Content-Type: ${result.headers["content-type"] || "not set"}`);
    console.log(`[discover]   Body length: ${result.body.length} bytes`);

    let parsed: any;
    try {
      parsed = JSON.parse(result.body);
    } catch {
      console.log(`[discover]   Body is NOT valid JSON. First 200 chars: ${result.body.substring(0, 200)}`);
      return;
    }

    // 4. Log top-level keys and types
    if (typeof parsed === "object" && parsed !== null) {
      const topKeys = Object.keys(parsed);
      console.log(`[discover]   Top-level keys: ${topKeys.join(", ")}`);
      for (const key of topKeys) {
        const val = parsed[key];
        const type = Array.isArray(val) ? `array[${val.length}]` : typeof val;
        const preview = typeof val === "string" ? val.substring(0, 80)
          : typeof val === "number" || typeof val === "boolean" ? String(val)
          : Array.isArray(val) ? `[${val.length} items]`
          : JSON.stringify(val).substring(0, 80);
        console.log(`[discover]     "${key}": ${type} = ${preview}`);
      }
    }

    // 5. Inspect event objects — log ALL fields, types, and sample values
    const events = Array.isArray(parsed) ? parsed : parsed.data || parsed.events || parsed.items || [];
    if (events.length > 0) {
      const sample = events[0];
      console.log("[discover] --- Event Object Schema (first event) ---");
      const fieldNames = Object.keys(sample);
      console.log(`[discover]   Fields (${fieldNames.length}): ${fieldNames.join(", ")}`);
      for (const field of fieldNames) {
        const val = sample[field];
        const type = val === null ? "null" : Array.isArray(val) ? `array[${val.length}]` : typeof val;
        const preview = val === null ? "null"
          : typeof val === "string" ? `"${val.substring(0, 100)}"`
          : typeof val === "number" || typeof val === "boolean" ? String(val)
          : JSON.stringify(val).substring(0, 100);
        console.log(`[discover]     ${field}: ${type} = ${preview}`);
      }

      // 6. Detect ID field — which field is used?
      const idCandidates = ["id", "eventId", "_id", "event_id", "ID", "Id"];
      const foundIdField = idCandidates.find((f) => sample[f] !== undefined);
      console.log(`[discover]   ID field detected: ${foundIdField ? `"${foundIdField}" = ${sample[foundIdField]}` : "NONE FOUND — check field names above!"}`);

      // 7. Detect timestamp field and format
      const tsCandidates = ["timestamp", "created_at", "createdAt", "time", "date", "ts", "eventTime", "occurred_at"];
      const foundTsField = tsCandidates.find((f) => sample[f] !== undefined);
      if (foundTsField) {
        const tsValue = sample[foundTsField];
        const isISO = typeof tsValue === "string" && /^\d{4}-\d{2}-\d{2}/.test(tsValue);
        const isUnixSec = typeof tsValue === "number" && tsValue > 1_000_000_000 && tsValue < 2_000_000_000;
        const isUnixMs = typeof tsValue === "number" && tsValue > 1_000_000_000_000;
        const format = isISO ? "ISO 8601" : isUnixSec ? "Unix seconds" : isUnixMs ? "Unix milliseconds" : "unknown";
        console.log(`[discover]   Timestamp field: "${foundTsField}" = ${tsValue} (format: ${format})`);
      } else {
        console.log(`[discover]   Timestamp field: NONE FOUND — check field names above!`);
      }

      // 8. Check for consistency across first few events
      if (events.length > 1) {
        const allFieldSets = events.slice(0, 5).map((e: any) => Object.keys(e).sort().join(","));
        const consistent = allFieldSets.every((f: string) => f === allFieldSets[0]);
        console.log(`[discover]   Schema consistency (first ${Math.min(5, events.length)} events): ${consistent ? "CONSISTENT" : "VARIES — fields differ between events!"}`);
      }
    } else {
      console.log("[discover]   No events array found in response body. Raw response (first 500 chars):");
      console.log(`[discover]   ${result.body.substring(0, 500)}`);
    }

    // 9. Inspect pagination cursor
    const cursor = parsed.nextCursor || parsed.cursor || parsed.next_cursor || parsed.next;
    if (cursor) {
      console.log("[discover] --- Cursor ---");
      console.log(`[discover]   Field: ${parsed.nextCursor ? "nextCursor" : parsed.cursor ? "cursor" : parsed.next_cursor ? "next_cursor" : "next"}`);
      console.log(`[discover]   Value: ${cursor}`);
      console.log(`[discover]   Length: ${String(cursor).length}`);
      const isBase64 = /^[A-Za-z0-9+/=]+$/.test(String(cursor));
      const isNumeric = /^\d+$/.test(String(cursor));
      const isUUID = /^[0-9a-f-]{36}$/i.test(String(cursor));
      console.log(`[discover]   Format: ${isBase64 ? "likely base64" : isNumeric ? "numeric offset" : isUUID ? "UUID" : "opaque string"}`);
    }

    // 10. hasMore field
    const hasMore = parsed.hasMore ?? parsed.has_more ?? parsed.hasNext ?? parsed.more;
    console.log(`[discover]   hasMore: ${hasMore} (field: ${parsed.hasMore !== undefined ? "hasMore" : parsed.has_more !== undefined ? "has_more" : parsed.hasNext !== undefined ? "hasNext" : "not found"})`);

    console.log("[discover] === END DEEP API INSPECTION ===");
  } catch (err) {
    console.log(`[discover] Deep inspection failed: ${String(err).substring(0, 200)}`);
  }
}

/**
 * Quick discovery - fires all probes in parallel for speed.
 */
export async function quickDiscover(): Promise<DiscoveryResult> {
  console.log("[discover] Running lean discovery (conserving rate limit budget)...");

  const result: DiscoveryResult = {
    maxLimit: 1000,
    typeFilterWorks: false,
    typeFilterParam: "type",
    bulkEndpoint: null,
    parallelCursorsWork: false,
    eventTypes: EVENT_TYPES,
    streamEndpoint: null,
    streamExpiresIn: 0,
    streamToken: null,
    streamTokenHeader: null,
    allEndpoint: null,
    idsEndpoint: null,
    batchEndpoint: null,
    dualRateLimitPools: true, // confirmed: header=10/60s, query=5/3s (separate pools)
    offsetWorks: false,
    offsetParam: "offset",
    timeRange: null,
    rateLimitBudget: {
      headerLimit: 10,
      headerWindow: 60,
      headerRemaining: 10,
      queryLimit: 5,
      queryWindow: 3,
      queryRemaining: 5,
      separatePools: true,
    },
    sseWorks: false,
    timeParamWorks: false,
    timeParam: "since",
    customHeaders: null,
  };

  // Phase 1: Single API call to inspect response shape + detect max page size
  // Use header auth for the first call (it's the slow pool, save query auth for ingestion)
  await probeApiShape();

  // Phase 2: Minimal probes — only test what we need
  // These use different auth or no-auth endpoints to preserve rate limit budget
  const [
    limitAndTypeResult,
    dashboardResult,
    dualAuthResult,
    streamAccessResult,
  ] = await Promise.allSettled([
    probeLimitAndTypeFilter(),
    probeDashboardAndBundle(),   // no auth needed for dashboard HTML + JS
    probeDualAuth(),
    requestStreamAccess(),       // get stream token for feed endpoint (no rate limits!)
  ]);

  // Limit/type info
  if (limitAndTypeResult.status === "fulfilled") {
    const ltResult = limitAndTypeResult.value;
    result.maxLimit = ltResult.maxLimit;
    result.typeFilterWorks = ltResult.typeFilterWorks;
    result.typeFilterParam = ltResult.typeFilterParam;
    console.log(`[discover] Max limit: ${result.maxLimit}, type filter: ${result.typeFilterWorks} (param: ${result.typeFilterParam})`);
  }

  // Dual-auth rate limit pools
  if (dualAuthResult.status === "fulfilled" && dualAuthResult.value) {
    result.dualRateLimitPools = dualAuthResult.value;
    if (result.dualRateLimitPools) {
      console.log("[discover] CONFIRMED: Dual rate limit pools (header vs query auth)!");
    }
  }

  // Stream access (feed endpoint — no rate limits!)
  if (streamAccessResult.status === "fulfilled" && streamAccessResult.value) {
    const sa = streamAccessResult.value;
    result.streamEndpoint = sa.endpoint;
    result.streamExpiresIn = sa.expiresIn;
    result.streamToken = sa.token;
    result.streamTokenHeader = sa.tokenHeader;
    console.log(`[discover] Stream access: endpoint=${sa.endpoint}, expiresIn=${sa.expiresIn}s, tokenHeader=${sa.tokenHeader}`);
  } else {
    console.log(`[discover] Stream access not available: ${streamAccessResult.status === "rejected" ? streamAccessResult.reason : "null result"}`);
  }

  // Dashboard/JS bundle discovery (no auth needed for HTML/JS, only for /internal/stats)
  if (dashboardResult.status === "fulfilled" && dashboardResult.value) {
    const dash = dashboardResult.value;
    if (dash.discoveredEndpoints.length > 0) {
      console.log(`[discover] Dashboard discovered ${dash.discoveredEndpoints.length} endpoints`);
    }
    if (dash.dataModelHints && (dash.dataModelHints as any).distributions?.eventTypes) {
      const statsTypes = ((dash.dataModelHints as any).distributions.eventTypes as Array<{ type: string; count: number }>)
        .map((et) => et.type)
        .filter((t) => t);
      if (statsTypes.length > 0 && statsTypes.length !== result.eventTypes.length) {
        console.log(`[discover] Updating event types from /internal/stats: ${statsTypes.join(", ")}`);
        result.eventTypes = statsTypes;
      }
    }
  }

  console.log("[discover] Discovery complete. Proceeding to ingestion.");
  return result;
}

async function probeBulkEndpoints(): Promise<string | null> {
  const hostBase = getHostBaseUrl();
  const apiKey = getApiKey();

  const endpoints = [
    `${hostBase}/internal/events/export?api_key=${apiKey}`,
    `${hostBase}/internal/events/dump?api_key=${apiKey}`,
    `${hostBase}/internal/events/all?api_key=${apiKey}`,
    `${hostBase}/internal/events?api_key=${apiKey}`,
    "/events/export",
    "/events/stream",
    "/events/bulk",
    "/events/download",
  ];

  const results = await Promise.allSettled(
    endpoints.map(async (ep) => {
      const r = await probeEndpoint(ep);
      if (r.ok && r.body.length > 500) {
        return ep;
      }
      return null;
    })
  );

  for (const r of results) {
    if (r.status === "fulfilled" && r.value) {
      return r.value;
    }
  }

  return null;
}

/**
 * Probe /events/all, /events/ids, /events/batch - these could be game-changers.
 */
async function probeFastPaths(): Promise<{
  allEndpoint: string | null;
  idsEndpoint: string | null;
  batchEndpoint: string | null;
}> {
  const res = { allEndpoint: null as string | null, idsEndpoint: null as string | null, batchEndpoint: null as string | null };

  const [allResult, idsResult, batchResult] = await Promise.allSettled([
    probeEndpoint("/events/all"),
    probeEndpoint("/events/ids"),
    probeEndpoint("/events/batch?limit=10000"),
  ]);

  if (allResult.status === "fulfilled" && allResult.value.ok) {
    const body = allResult.value.body;
    // Check if it returned substantial data (array or object with data)
    try {
      const parsed = JSON.parse(body);
      const events = Array.isArray(parsed) ? parsed : parsed.data || [];
      if (events.length > 100) {
        res.allEndpoint = "/events/all";
        console.log(`[discover] /events/all probe: ${events.length} events in first response`);
      }
    } catch {
      // Could be NDJSON
      if (body.length > 1000) {
        res.allEndpoint = "/events/all";
      }
    }
  }

  if (idsResult.status === "fulfilled" && idsResult.value.ok) {
    const body = idsResult.value.body;
    try {
      const parsed = JSON.parse(body);
      const ids = Array.isArray(parsed) ? parsed : parsed.data || parsed.ids || [];
      if (ids.length > 0) {
        res.idsEndpoint = "/events/ids";
        console.log(`[discover] /events/ids probe: ${ids.length} IDs in first response`);
      }
    } catch {
      // Newline-delimited IDs?
      const lines = body.split("\n").filter((l: string) => l.trim());
      if (lines.length > 10) {
        res.idsEndpoint = "/events/ids";
      }
    }
  }

  if (batchResult.status === "fulfilled" && batchResult.value.ok) {
    const body = batchResult.value.body;
    try {
      const parsed = JSON.parse(body);
      const events = Array.isArray(parsed) ? parsed : parsed.data || [];
      if (events.length > 0) {
        res.batchEndpoint = "/events/batch";
        console.log(`[discover] /events/batch probe: ${events.length} events`);
      }
    } catch {
      // ignore
    }
  }

  return res;
}

/**
 * Probe whether header auth and query auth have separate rate limit pools.
 */
async function probeDualAuth(): Promise<boolean> {
  try {
    const [headerResult, queryResult] = await Promise.all([
      apiRequest("/events?limit=1", { authMethod: "header" }),
      apiRequest("/events?limit=1", { authMethod: "query" }),
    ]);

    const hRemaining = headerResult.rateLimit.remaining;
    const qRemaining = queryResult.rateLimit.remaining;
    const hLimit = headerResult.rateLimit.limit;
    const qLimit = queryResult.rateLimit.limit;

    console.log(`[discover] Rate limits - Header: ${hRemaining}/${hLimit}, Query: ${qRemaining}/${qLimit}`);

    // If limits or remaining differ significantly, they're separate pools
    if (hLimit !== qLimit || Math.abs(hRemaining - qRemaining) > 2) {
      return true;
    }

    // Same pool if both decremented from same starting point
    return false;
  } catch {
    return false;
  }
}

async function probeLimitAndTypeFilter(): Promise<{
  maxLimit: number;
  typeFilterWorks: boolean;
  typeFilterParam: string;
}> {
  let maxLimit = 1000;
  let typeFilterWorks = false;
  let typeFilterParam = "type";

  // Single call: test limit=5000 (known max from manual testing)
  try {
    const limitResult = await fetchEvents(undefined, 5000);
    const count = limitResult.events.data?.length || 0;
    if (count >= 4500) {
      maxLimit = 5000;
      console.log(`[discover] limit=5000 works! Got ${count} events`);
    } else if (count > 1000) {
      maxLimit = count;
      console.log(`[discover] limit=5000 capped at ${count} events`);
    }

    // Check if events have a type field (from the data we already fetched)
    const data = limitResult.events.data || [];
    if (data.length > 0 && data[0].type) {
      // Verify type filtering works by checking if any event has a type field
      console.log(`[discover] Events have "type" field — will test type filtering`);
    }
  } catch (err) {
    console.log(`[discover] limit probe failed: ${String(err).substring(0, 100)}`);
  }

  // Test type filtering with a single request
  try {
    const typeResult = await fetchEvents(undefined, 10, { type: "page_view" });
    const data = typeResult.events.data || [];
    if (data.length > 0) {
      const allMatch = data.every(
        (e: Record<string, unknown>) =>
          e.type === "page_view" || e.eventType === "page_view" || e.event_type === "page_view"
      );
      if (allMatch) {
        typeFilterWorks = true;
        typeFilterParam = "type";
        console.log(`[discover] Type filtering confirmed: ?type=page_view works`);
      }
    }
  } catch {
    // Type filtering not available
  }

  return { maxLimit, typeFilterWorks, typeFilterParam };
}

async function inspectResponseHeaders(): Promise<{
  streamUrl: string | null;
  allHeaders: string;
} | null> {
  try {
    const resp = await apiRequest("/events?limit=1");
    const headers = resp.headers;
    const headerStr = Object.entries(headers)
      .map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(", ") : v}`)
      .join(" | ");

    let streamUrl: string | null = null;

    const linkHeader = headers["link"] || headers["Link"];
    if (linkHeader) {
      const linkStr = String(linkHeader);
      console.log(`[discover] Link header found: ${linkStr}`);
      const streamMatch = linkStr.match(/<([^>]+)>;\s*rel="(?:stream|export|bulk|download)"/);
      if (streamMatch) {
        streamUrl = streamMatch[1];
      }
    }

    for (const [key, value] of Object.entries(headers)) {
      const lk = key.toLowerCase();
      if (
        lk.includes("stream") ||
        lk.includes("export") ||
        lk.includes("bulk") ||
        lk.includes("download") ||
        lk.includes("alternate") ||
        lk.includes("location")
      ) {
        console.log(`[discover] Interesting header: ${key}: ${value}`);
        if (!streamUrl && String(value).startsWith("http")) {
          streamUrl = String(value);
        }
      }
    }

    return { streamUrl, allHeaders: headerStr };
  } catch {
    return null;
  }
}

async function probeExtraFormats(): Promise<{
  ndjsonWorks: boolean;
  fieldsWork: boolean;
  totalCount: number;
  countEndpoint: number;
}> {
  const res = { ndjsonWorks: false, fieldsWork: false, totalCount: 0, countEndpoint: 0 };

  const [ndjsonResult, fieldsResult, countResult] = await Promise.allSettled([
    probeEndpoint("/events?limit=10", { acceptHeader: "application/ndjson" }),
    probeEndpoint("/events?limit=10&fields=id"),
    probeEndpoint("/events/count"),
  ]);

  if (ndjsonResult.status === "fulfilled" && ndjsonResult.value.ok) {
    const ct = ndjsonResult.value.contentType;
    res.ndjsonWorks = ct.includes("ndjson") || ct.includes("x-ndjson");
  }

  if (fieldsResult.status === "fulfilled" && fieldsResult.value.ok) {
    try {
      const parsed = JSON.parse(fieldsResult.value.body);
      const data = parsed.data || parsed;
      if (Array.isArray(data) && data.length > 0) {
        const keys = Object.keys(data[0]);
        res.fieldsWork = keys.length <= 3;
      }
    } catch {
      // ignore
    }
  }

  if (countResult.status === "fulfilled" && countResult.value.ok) {
    try {
      const parsed = JSON.parse(countResult.value.body);
      res.countEndpoint = parsed.count || parsed.total || 0;
    } catch {
      // ignore
    }
  }

  try {
    const resp = await apiRequest("/events?limit=1");
    const totalCountHeader = resp.headers["x-total-count"] || resp.headers["X-Total-Count"];
    if (totalCountHeader) {
      res.totalCount = parseInt(String(totalCountHeader), 10) || 0;
    }
  } catch {
    // ignore
  }

  return res;
}

/**
 * Probe whether offset/page/skip pagination is supported.
 */
async function probeOffsetPagination(): Promise<{ works: boolean; param: string }> {
  const params = ["offset", "page", "skip"];

  for (const param of params) {
    try {
      // Fetch with offset=0 and offset=100 — if both return data AND they differ, it works
      const [r0, r1] = await Promise.all([
        fetchEvents(undefined, 10, { [param]: "0" }),
        fetchEvents(undefined, 10, { [param]: "100" }),
      ]);

      const d0 = r0.events.data || [];
      const d1 = r1.events.data || [];

      if (d0.length > 0 && d1.length > 0) {
        // Check if the data is actually different (offset is working)
        const id0 = d0[0]?.id || d0[0]?.eventId;
        const id1 = d1[0]?.id || d1[0]?.eventId;
        if (id0 && id1 && id0 !== id1) {
          return { works: true, param };
        }
      }
    } catch {
      // Try next param
    }
  }

  return { works: false, param: "offset" };
}

/**
 * Probe for event time range by fetching first and last events.
 */
async function probeTimeRange(): Promise<{ min: string; max: string } | null> {
  try {
    // Try sorting by timestamp to get range boundaries
    const [firstResult, lastResult] = await Promise.allSettled([
      fetchEvents(undefined, 1, { sort: "timestamp", order: "asc" }),
      fetchEvents(undefined, 1, { sort: "timestamp", order: "desc" }),
    ]);

    let min: string | null = null;
    let max: string | null = null;

    if (firstResult.status === "fulfilled") {
      const d = firstResult.value.events.data || [];
      if (d.length > 0) {
        min = String(d[0].timestamp || d[0].created_at || d[0].createdAt || "");
      }
    }

    if (lastResult.status === "fulfilled") {
      const d = lastResult.value.events.data || [];
      if (d.length > 0) {
        max = String(d[0].timestamp || d[0].created_at || d[0].createdAt || "");
      }
    }

    // If sorting didn't work, just get timestamps from a normal fetch
    if (!min || !max || min === "undefined" || max === "undefined") {
      const normal = await fetchEvents(undefined, 100);
      const data = normal.events.data || [];
      if (data.length > 0) {
        const timestamps = data
          .map((e) => String(e.timestamp || e.created_at || e.createdAt || ""))
          .filter((t) => t && t !== "undefined")
          .sort();
        if (timestamps.length > 0) {
          // We only see a tiny window, not useful for sharding
          return null;
        }
      }
      return null;
    }

    if (min && max && min !== "undefined" && max !== "undefined") {
      return { min, max };
    }

    return null;
  } catch {
    return null;
  }
}

// =============================================================================
// Rate Limit Budget Measurement
// =============================================================================

async function measureRateLimitBudget(): Promise<RateLimitBudget | null> {
  try {
    // Make 3 rapid requests with header auth to measure the pool
    const headerSamples: Array<{ limit: number; remaining: number; reset: number }> = [];
    for (let i = 0; i < 3; i++) {
      const r = await apiRequest("/events?limit=1", { authMethod: "header" });
      headerSamples.push({
        limit: r.rateLimit.limit,
        remaining: r.rateLimit.remaining,
        reset: r.rateLimit.reset,
      });
    }

    // Make 3 rapid requests with query auth
    const querySamples: Array<{ limit: number; remaining: number; reset: number }> = [];
    for (let i = 0; i < 3; i++) {
      const r = await apiRequest("/events?limit=1", { authMethod: "query" });
      querySamples.push({
        limit: r.rateLimit.limit,
        remaining: r.rateLimit.remaining,
        reset: r.rateLimit.reset,
      });
    }

    const hLimit = headerSamples[0]?.limit || 100;
    const qLimit = querySamples[0]?.limit || 50;
    const hRemaining = headerSamples[headerSamples.length - 1]?.remaining || 0;
    const qRemaining = querySamples[querySamples.length - 1]?.remaining || 0;

    // Compute window from reset timestamps
    const now = Math.floor(Date.now() / 1000);
    const hReset = headerSamples[0]?.reset || now + 60;
    const qReset = querySamples[0]?.reset || now + 60;
    const hWindow = Math.max(hReset - now, 1);
    const qWindow = Math.max(qReset - now, 1);

    // Determine if pools are separate: if header remaining didn't drop when query was hit
    // Compare: after 3 header + 3 query requests, if header remaining is still near
    // what it was after just 3 header requests, the pools are separate
    const hRemainingAfterAll = (await apiRequest("/events?limit=1", { authMethod: "header" })).rateLimit.remaining;
    // If header remaining dropped by ~4 (3 header + 1 check), pools are shared
    // If header remaining dropped by ~1 (just the check), pools are separate
    const headerDrop = hRemaining - hRemainingAfterAll;
    const separatePools = headerDrop <= 2; // Only the check request should have consumed

    console.log(`[discover] Rate budget: header=${hLimit}/${hWindow}s (rem=${hRemainingAfterAll}), query=${qLimit}/${qWindow}s (rem=${qRemaining}), separate=${separatePools}, headerDrop=${headerDrop}`);

    return {
      headerLimit: hLimit,
      headerWindow: hWindow,
      headerRemaining: hRemainingAfterAll,
      queryLimit: qLimit,
      queryWindow: qWindow,
      queryRemaining: qRemaining,
      separatePools,
    };
  } catch (err) {
    console.log(`[discover] Rate budget measurement failed: ${String(err).substring(0, 100)}`);
    return null;
  }
}

// =============================================================================
// Custom Header Probes
// =============================================================================

async function probeCustomHeaders(): Promise<Record<string, string> | null> {
  const headersToTest: Array<Record<string, string>> = [
    { "X-Batch-Mode": "true" },
    { "X-Priority": "high" },
    { "X-Internal": "true" },
    { "X-Bulk": "true" },
    { "X-No-Limit": "true" },
    { "X-Stream": "true" },
  ];

  // Get baseline rate limit
  let baselineLimit = 0;
  try {
    const baseline = await apiRequest("/events?limit=1000");
    baselineLimit = baseline.rateLimit.limit;
  } catch {
    return null;
  }

  for (const extraHeaders of headersToTest) {
    try {
      const result = await apiRequest("/events?limit=1000", { headers: extraHeaders });
      // Check if rate limit is higher or response is larger
      if (result.rateLimit.limit > baselineLimit) {
        console.log(`[discover] Custom header ${JSON.stringify(extraHeaders)} increased rate limit from ${baselineLimit} to ${result.rateLimit.limit}`);
        return extraHeaders;
      }
    } catch {
      // ignore
    }
  }

  return null;
}

// =============================================================================
// SSE Probe
// =============================================================================

async function probeSSE(): Promise<boolean> {
  try {
    const result = await probeEndpoint("/events/sse", { acceptHeader: "text/event-stream" });
    if (result.ok && (result.body.includes("data:") || result.contentType.includes("event-stream"))) {
      console.log(`[discover] SSE probe: content-type=${result.contentType}, body starts with: ${result.body.substring(0, 100)}`);
      return true;
    }
    return false;
  } catch {
    return false;
  }
}

// =============================================================================
// Time-Based Parameter Probes
// =============================================================================

async function probeTimeParams(): Promise<{ works: boolean; param: string } | null> {
  const params = ["since", "until", "from", "to", "start_time", "end_time", "after", "before"];

  // First get a reference event with a timestamp
  let refTimestamp: string | null = null;
  try {
    const ref = await fetchEvents(undefined, 5);
    const data = ref.events.data || [];
    if (data.length > 0) {
      refTimestamp = String(data[0].timestamp || data[0].created_at || data[0].createdAt || "");
    }
  } catch {
    return null;
  }

  if (!refTimestamp || refTimestamp === "undefined") return null;

  // Test each time param
  for (const param of params) {
    try {
      const result = await fetchEvents(undefined, 10, { [param]: refTimestamp });
      const data = result.events.data || [];
      if (data.length > 0) {
        // Compare with unfiltered — if the first ID differs, the param is doing something
        const unfilteredResult = await fetchEvents(undefined, 10);
        const unfilteredData = unfilteredResult.events.data || [];
        const filteredId = data[0]?.id || data[0]?.eventId;
        const unfilteredId = unfilteredData[0]?.id || unfilteredData[0]?.eventId;

        if (filteredId && unfilteredId && filteredId !== unfilteredId) {
          return { works: true, param };
        }
      }
    } catch {
      // try next
    }
  }

  return { works: false, param: "since" };
}

// =============================================================================
// Dashboard & JS Bundle Discovery
// =============================================================================

/**
 * Fetch the dashboard HTML page, find JS bundle URLs, parse them for fetch() calls
 * and API endpoints. Also deeply inspect /internal/stats for data model hints.
 * This implements: "curious developers explore everything, even data model by
 * looking at dashboard and inspect calls"
 */
async function probeDashboardAndBundle(): Promise<{
  discoveredEndpoints: string[];
  dataModelHints: Record<string, unknown>;
}> {
  const hostBase = getHostBaseUrl();
  const result: { discoveredEndpoints: string[]; dataModelHints: Record<string, unknown> } = {
    discoveredEndpoints: [],
    dataModelHints: {},
  };

  // 1. Fetch dashboard HTML and find JS bundle paths
  try {
    const { statusCode, body: bodyStream } = await undiciRequest(hostBase, {
      method: "GET",
      headers: { Accept: "text/html" },
      headersTimeout: 10_000,
      bodyTimeout: 30_000,
    });
    const html = await bodyStream.text();

    if (statusCode === 200 && html.length > 0) {
      console.log(`[discover] Dashboard HTML fetched (${html.length} bytes)`);

      // Find script tags with src attributes
      const scriptMatches = [...html.matchAll(/<script[^>]+src="([^"]+)"/g)];
      const bundleUrls: string[] = scriptMatches
        .map((m) => m[1])
        .filter((src) => src.endsWith(".js") || src.includes("assets/"));

      console.log(`[discover] Found ${bundleUrls.length} JS bundle(s): ${bundleUrls.join(", ")}`);

      // 2. Fetch each JS bundle and scan for API patterns
      for (const bundlePath of bundleUrls.slice(0, 3)) { // limit to 3 bundles
        try {
          const bundleUrl = bundlePath.startsWith("http") ? bundlePath : `${hostBase}${bundlePath.startsWith("/") ? "" : "/"}${bundlePath}`;
          const { statusCode: bStatus, body: bBodyStream } = await undiciRequest(bundleUrl, {
            method: "GET",
            headers: { Accept: "application/javascript" },
            headersTimeout: 10_000,
            bodyTimeout: 60_000,
          });
          const bundleCode = await bBodyStream.text();

          if (bStatus === 200 && bundleCode.length > 0) {
            console.log(`[discover] JS bundle ${bundlePath} fetched (${(bundleCode.length / 1024).toFixed(0)} KB)`);

            // Scan for fetch() calls — extract URLs
            const fetchMatches = [...bundleCode.matchAll(/fetch\s*\(\s*["'`]([^"'`]+)["'`]/g)];
            const fetchUrls = fetchMatches.map((m) => m[1]);
            console.log(`[discover] Found ${fetchUrls.length} fetch() URLs in bundle:`);
            for (const url of fetchUrls) {
              console.log(`[discover]   fetch("${url}")`);
              if (url.startsWith("/api/") || url.startsWith("/internal/")) {
                result.discoveredEndpoints.push(url);
              }
            }

            // Scan for API path patterns: /api/v1/something or /internal/something
            const pathMatches = [...bundleCode.matchAll(/["'`](\/(?:api\/v\d+|internal)\/[a-zA-Z0-9/_-]+)["'`]/g)];
            const uniquePaths = [...new Set(pathMatches.map((m) => m[1]))];
            for (const p of uniquePaths) {
              if (!result.discoveredEndpoints.includes(p)) {
                result.discoveredEndpoints.push(p);
                console.log(`[discover]   API path in bundle: "${p}"`);
              }
            }

            // Scan for query parameter patterns: ?param= or &param=
            const paramMatches = [...bundleCode.matchAll(/[?&]([a-zA-Z_]+)=/g)];
            const uniqueParams = [...new Set(paramMatches.map((m) => m[1]))];
            if (uniqueParams.length > 0) {
              console.log(`[discover]   Query params found in bundle: ${uniqueParams.join(", ")}`);
            }

            // Scan for header names in objects — "X-Something" or "Content-Type"
            const headerMatches = [...bundleCode.matchAll(/["']([A-Z][a-zA-Z]+-[A-Za-z-]+)["']\s*:/g)];
            const uniqueHeaders = [...new Set(headerMatches.map((m) => m[1]))];
            if (uniqueHeaders.length > 0) {
              console.log(`[discover]   HTTP headers in bundle: ${uniqueHeaders.join(", ")}`);
            }
          }
        } catch (err) {
          console.log(`[discover] Failed to fetch bundle ${bundlePath}: ${String(err).substring(0, 100)}`);
        }
      }
    }
  } catch (err) {
    console.log(`[discover] Dashboard fetch failed: ${String(err).substring(0, 100)}`);
  }

  // 3. Deep inspection of /internal/stats — requires API key auth
  try {
    const apiKey = getApiKey();
    const { statusCode, body: bodyStream } = await undiciRequest(`${hostBase}/internal/stats`, {
      method: "GET",
      headers: { Accept: "application/json", "X-API-Key": apiKey },
      headersTimeout: 10_000,
      bodyTimeout: 10_000,
    });
    const statsBody = await bodyStream.text();

    if (statusCode === 200) {
      const stats = JSON.parse(statsBody);
      console.log("[discover] === /internal/stats Deep Inspection ===");
      console.log(`[discover]   Full response: ${JSON.stringify(stats, null, 2).substring(0, 2000)}`);

      // Store all stats as data model hints
      result.dataModelHints = stats;

      // Log counts
      if (stats.counts) {
        console.log("[discover]   Counts:");
        for (const [key, value] of Object.entries(stats.counts)) {
          console.log(`[discover]     ${key}: ${value}`);
        }
      }

      // Log distributions — these tell us about event types, which is critical for parallel workers
      if (stats.distributions) {
        if (stats.distributions.eventTypes) {
          console.log("[discover]   Event type distribution:");
          for (const et of stats.distributions.eventTypes) {
            console.log(`[discover]     ${(et as any).type}: ${(et as any).count?.toLocaleString()}`);
          }
        }
        if (stats.distributions.deviceTypes) {
          console.log("[discover]   Device type distribution:");
          for (const dt of stats.distributions.deviceTypes) {
            console.log(`[discover]     ${(dt as any).type}: ${(dt as any).count?.toLocaleString()}`);
          }
        }
        // Log any OTHER distributions we haven't seen
        for (const [key, value] of Object.entries(stats.distributions)) {
          if (key !== "eventTypes" && key !== "deviceTypes") {
            console.log(`[discover]   Distribution "${key}": ${JSON.stringify(value).substring(0, 200)}`);
          }
        }
      }

      // Log cache info
      if (stats.cache) {
        console.log(`[discover]   Cache: ${JSON.stringify(stats.cache)}`);
      }

      // Log ANY unexpected top-level keys — these could be hints
      const expectedKeys = ["counts", "distributions", "cache"];
      for (const key of Object.keys(stats)) {
        if (!expectedKeys.includes(key)) {
          console.log(`[discover]   UNEXPECTED key in stats: "${key}" = ${JSON.stringify(stats[key]).substring(0, 200)}`);
        }
      }

      console.log("[discover] === End /internal/stats ===");
    }
  } catch (err) {
    console.log(`[discover] /internal/stats failed: ${String(err).substring(0, 100)}`);
  }

  // 4. Deep inspection of /internal/health
  try {
    const { statusCode, body: bodyStream } = await undiciRequest(`${hostBase}/internal/health`, {
      method: "GET",
      headers: { Accept: "application/json" },
      headersTimeout: 10_000,
      bodyTimeout: 10_000,
    });
    const healthBody = await bodyStream.text();

    if (statusCode === 200) {
      const health = JSON.parse(healthBody);
      console.log("[discover] === /internal/health ===");
      console.log(`[discover]   ${JSON.stringify(health, null, 2).substring(0, 1000)}`);
      console.log("[discover] === End /internal/health ===");
    }
  } catch (err) {
    console.log(`[discover] /internal/health failed: ${String(err).substring(0, 100)}`);
  }

  if (result.discoveredEndpoints.length > 0) {
    console.log(`[discover] Dashboard discovery found ${result.discoveredEndpoints.length} endpoints: ${result.discoveredEndpoints.join(", ")}`);
  }

  return result;
}

// Allow running standalone
if (require.main === module) {
  quickDiscover()
    .then((r) => {
      console.log("\n[discover] Result:", JSON.stringify(r, null, 2));
    })
    .catch(console.error);
}
