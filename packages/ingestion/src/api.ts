/**
 * API client with dual-auth support, independent rate limit tracking,
 * and connection pooling for maximum parallel throughput.
 */

import { Agent, request as undiciRequest } from "undici";

const API_BASE_URL =
  process.env.API_BASE_URL ||
  "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1";
const API_KEY = process.env.TARGET_API_KEY || "";

// Connection pool agent for keep-alive - aggressive settings for max throughput
const agent = new Agent({
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
  pipelining: 6,
  connections: 100,
});

export interface EventsResponse {
  data: Array<Record<string, unknown>>;
  hasMore: boolean;
  nextCursor?: string;
  [key: string]: unknown;
}

export interface RateLimitInfo {
  limit: number;
  remaining: number;
  reset: number;
  retryAfter?: number;
}

export type AuthMethod = "header" | "query";

// Independent rate limit state per auth method
interface RateLimitState {
  remaining: number;
  resetAt: number; // epoch ms when rate limit resets
}

const rateLimits: Record<AuthMethod, RateLimitState> = {
  header: { remaining: 50, resetAt: 0 },
  query: { remaining: 50, resetAt: 0 },
};

// =============================================================================
// Global concurrency limiter (shared across all workers) — adaptive
// =============================================================================
let inFlight = 0;
let maxInFlight = parseInt(process.env.MAX_IN_FLIGHT || "40", 10);
const waitQueue: Array<() => void> = [];

export function getMaxInFlight(): number {
  return maxInFlight;
}

export function setMaxInFlight(n: number): void {
  const old = maxInFlight;
  maxInFlight = Math.max(5, Math.min(n, 150));
  // If we increased, wake up queued waiters
  if (maxInFlight > old) {
    while (waitQueue.length > 0 && inFlight < maxInFlight) {
      const next = waitQueue.shift()!;
      next();
    }
  }
  if (maxInFlight !== old) {
    console.log(`[api] Concurrency adjusted: ${old} → ${maxInFlight}`);
  }
}

export async function acquireSlot(): Promise<void> {
  if (inFlight < maxInFlight) {
    inFlight++;
    return;
  }
  await new Promise<void>((resolve) => waitQueue.push(resolve));
  inFlight++;
}

export function releaseSlot(): void {
  inFlight--;
  if (waitQueue.length > 0 && inFlight < maxInFlight) {
    const next = waitQueue.shift()!;
    next();
  }
}

// =============================================================================
// Throughput Tracker — measures events/sec per strategy in real-time
// =============================================================================

interface ThroughputSample {
  ts: number;
  events: number;
  strategy: string;
}

class ThroughputTrackerClass {
  private samples: ThroughputSample[] = [];
  private maxSamples = 1000;

  record(events: number, strategy: string): void {
    this.samples.push({ ts: Date.now(), events, strategy });
    if (this.samples.length > this.maxSamples) {
      this.samples = this.samples.slice(-this.maxSamples);
    }
  }

  /** events/sec over the last windowMs for a given strategy (or all) */
  getRate(strategy?: string, windowMs = 10000): number {
    const cutoff = Date.now() - windowMs;
    let totalEvents = 0;
    let earliest = Date.now();

    for (const s of this.samples) {
      if (s.ts < cutoff) continue;
      if (strategy && s.strategy !== strategy) continue;
      totalEvents += s.events;
      if (s.ts < earliest) earliest = s.ts;
    }

    const elapsed = (Date.now() - earliest) / 1000;
    return elapsed > 0 ? totalEvents / elapsed : 0;
  }

  getBestStrategy(): string | null {
    const strategies = new Set(this.samples.map((s) => s.strategy));
    let best: string | null = null;
    let bestRate = 0;
    for (const s of strategies) {
      const rate = this.getRate(s);
      if (rate > bestRate) {
        bestRate = rate;
        best = s;
      }
    }
    return best;
  }
}

export const throughputTracker = new ThroughputTrackerClass();

// =============================================================================
// Proactive Rate Budget Pacer — fires requests at computed interval
// =============================================================================

export class RateBudgetPacer {
  private headerRPS: number;
  private queryRPS: number;
  private totalRPS: number;
  private intervalMs: number;
  private lastFire = 0;
  private counter = 0; // for rotating auth methods
  private headerRatio: number; // proportion of requests using header auth

  constructor(budget: {
    headerLimit: number;
    headerWindow: number;
    queryLimit: number;
    queryWindow: number;
    separatePools: boolean;
  }) {
    this.headerRPS = budget.headerLimit / Math.max(budget.headerWindow, 1);
    this.queryRPS = budget.separatePools
      ? budget.queryLimit / Math.max(budget.queryWindow, 1)
      : 0; // if shared pool, don't double-count
    this.totalRPS = this.headerRPS + this.queryRPS;
    // Leave 10% headroom to avoid hitting 429
    this.intervalMs = Math.max(1000 / (this.totalRPS * 0.9), 10);
    this.headerRatio = this.headerRPS / Math.max(this.totalRPS, 0.01);

    console.log(
      `[pacer] Initialized: headerRPS=${this.headerRPS.toFixed(1)}, queryRPS=${this.queryRPS.toFixed(1)}, ` +
      `totalRPS=${this.totalRPS.toFixed(1)}, intervalMs=${this.intervalMs.toFixed(0)}, ` +
      `headerRatio=${(this.headerRatio * 100).toFixed(0)}%`
    );
  }

  /** Wait until next slot, return which auth method to use */
  async acquirePacedSlot(): Promise<AuthMethod> {
    const now = Date.now();
    const elapsed = now - this.lastFire;
    if (elapsed < this.intervalMs) {
      await new Promise((r) => setTimeout(r, this.intervalMs - elapsed));
    }
    this.lastFire = Date.now();

    // Rotate auth methods proportionally
    this.counter++;
    const useHeader = (this.counter % Math.round(1 / this.headerRatio)) === 0 || this.queryRPS === 0;
    return useHeader ? "header" : "query";
  }

  getTotalRPS(): number {
    return this.totalRPS;
  }
}

// =============================================================================
// First-request logging — log full details on first few API calls
// =============================================================================
let requestCount = 0;
const LOG_FIRST_N = 3; // Log full details for first 3 requests

function logFirstRequest(
  method: string,
  url: string,
  statusCode: number,
  headers: Record<string, string | string[]>,
  bodyPreview: string,
  authMethod: AuthMethod
): void {
  requestCount++;
  if (requestCount > LOG_FIRST_N) return;

  console.log(`[api] === Request #${requestCount} Details ===`);
  console.log(`[api]   ${method} ${url} (auth: ${authMethod})`);
  console.log(`[api]   Status: ${statusCode}`);
  console.log(`[api]   Response headers:`);
  for (const [k, v] of Object.entries(headers)) {
    const val = Array.isArray(v) ? v.join(", ") : v;
    console.log(`[api]     ${k}: ${val}`);
  }
  console.log(`[api]   Body preview (first 300 chars): ${bodyPreview.substring(0, 300)}`);
  console.log(`[api] === End Request #${requestCount} ===`);
}

function updateRateLimit(method: AuthMethod, info: RateLimitInfo): void {
  rateLimits[method].remaining = info.remaining;
  // reset is seconds from epoch
  if (info.reset > 0) {
    rateLimits[method].resetAt = info.reset * 1000;
  }
  if (info.retryAfter) {
    rateLimits[method].resetAt = Date.now() + info.retryAfter * 1000;
  }
}

export function getRateLimitState(): Record<AuthMethod, RateLimitState> {
  return rateLimits;
}

/**
 * Pick the best auth method - prefer the one with more remaining requests.
 * If one is exhausted, use the other.
 */
export function pickAuthMethod(preferred?: AuthMethod): AuthMethod {
  const now = Date.now();
  const headerOk = rateLimits.header.remaining > 1 || now >= rateLimits.header.resetAt;
  const queryOk = rateLimits.query.remaining > 1 || now >= rateLimits.query.resetAt;

  if (preferred && ((preferred === "header" && headerOk) || (preferred === "query" && queryOk))) {
    return preferred;
  }

  if (headerOk && !queryOk) return "header";
  if (queryOk && !headerOk) return "query";
  if (headerOk && queryOk) {
    // Prefer header (better rate limits per CLAUDE.md)
    return rateLimits.header.remaining >= rateLimits.query.remaining ? "header" : "query";
  }

  // Both exhausted - return whichever resets first
  return rateLimits.header.resetAt <= rateLimits.query.resetAt ? "header" : "query";
}

/**
 * Sleep until at least one auth method is available. Returns immediately if one is available.
 */
export async function waitForRateLimit(): Promise<void> {
  const now = Date.now();
  const headerOk = rateLimits.header.remaining > 1 || now >= rateLimits.header.resetAt;
  const queryOk = rateLimits.query.remaining > 1 || now >= rateLimits.query.resetAt;

  if (headerOk || queryOk) return;

  const waitUntil = Math.min(rateLimits.header.resetAt, rateLimits.query.resetAt);
  const waitMs = Math.max(waitUntil - now, 100);
  console.log(`[api] Both auth methods rate-limited. Waiting ${waitMs}ms`);
  await sleep(waitMs);
}

function parseRateLimitHeaders(headers: Record<string, string | string[]>): RateLimitInfo {
  const get = (key: string): string => {
    const v = headers[key];
    return Array.isArray(v) ? v[0] : v || "0";
  };
  return {
    limit: parseInt(get("x-ratelimit-limit") || get("ratelimit-limit") || "0", 10),
    remaining: parseInt(get("x-ratelimit-remaining") || get("ratelimit-remaining") || "0", 10),
    reset: parseInt(get("x-ratelimit-reset") || get("ratelimit-reset") || "0", 10),
    retryAfter: headers["retry-after"]
      ? parseInt(String(headers["retry-after"]), 10)
      : undefined,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Build URL with auth. For "header" auth, url stays clean. For "query" auth, append apiKey param.
 */
function buildUrl(path: string, authMethod: AuthMethod): string {
  let url = path.startsWith("http") ? path : `${API_BASE_URL}${path}`;
  if (authMethod === "query") {
    const sep = url.includes("?") ? "&" : "?";
    url += `${sep}api_key=${encodeURIComponent(API_KEY)}`;
  }
  return url;
}

function buildHeaders(authMethod: AuthMethod, extra?: Record<string, string>, acceptHeader?: string): Record<string, string> {
  const h: Record<string, string> = {
    Accept: acceptHeader || "application/json",
    ...extra,
  };
  if (authMethod === "header") {
    h["X-API-Key"] = API_KEY;
  }
  return h;
}

export interface ApiResult {
  statusCode: number;
  headers: Record<string, string | string[]>;
  body: string;
  rateLimit: RateLimitInfo;
  authMethod: AuthMethod;
}

/**
 * Make an authenticated API request with rate limit handling, retries, and dual-auth.
 */
export async function apiRequest(
  path: string,
  options: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
    acceptHeader?: string;
    authMethod?: AuthMethod;
  } = {}
): Promise<ApiResult> {
  const method = options.method || "GET";
  const maxRetries = 5;
  let authMethod = options.authMethod || pickAuthMethod();

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const url = buildUrl(path, authMethod);
    const reqHeaders = buildHeaders(authMethod, options.headers, options.acceptHeader);

    try {
      const { statusCode, headers: respHeaders, body: bodyStream } = await undiciRequest(url, {
        method: method as any,
        headers: reqHeaders,
        body: options.body,
        dispatcher: agent,
        headersTimeout: 30_000,
        bodyTimeout: 120_000,
      });

      const rawHeaders: Record<string, string | string[]> = {};
      const hKeys = Object.keys(respHeaders);
      for (const k of hKeys) {
        const v = respHeaders[k];
        if (v !== undefined) rawHeaders[k] = v;
      }

      const rateLimit = parseRateLimitHeaders(rawHeaders);
      updateRateLimit(authMethod, rateLimit);

      // Fail-fast on auth errors — no point retrying with the same key
      if (statusCode === 401 || statusCode === 403) {
        const errBody = await bodyStream.text();
        throw new Error(`[api] AUTH FAILED (${statusCode}): ${errBody.substring(0, 200)}`);
      }

      if (statusCode === 429) {
        // Rate limited - try switching auth method
        const otherMethod: AuthMethod = authMethod === "header" ? "query" : "header";
        const otherOk = rateLimits[otherMethod].remaining > 1 || Date.now() >= rateLimits[otherMethod].resetAt;
        // Consume the body
        await bodyStream.text();

        if (otherOk) {
          authMethod = otherMethod;
          continue;
        }

        // Both exhausted, wait
        const waitMs = Math.max((rateLimit.retryAfter || 1) * 1000, 200);
        console.log(`[api] Rate limited (both methods). Waiting ${waitMs}ms (attempt ${attempt + 1})`);
        await sleep(waitMs);
        continue;
      }

      const bodyText = await bodyStream.text();

      // Log full details for first few requests
      logFirstRequest(method, url, statusCode, rawHeaders, bodyText, authMethod);

      if (statusCode >= 500 && attempt < maxRetries - 1) {
        const waitMs = Math.min(1000 * Math.pow(2, attempt), 10000);
        await sleep(waitMs);
        continue;
      }

      return { statusCode, headers: rawHeaders, body: bodyText, rateLimit, authMethod };
    } catch (err) {
      if (attempt < maxRetries - 1) {
        await sleep(1000 * (attempt + 1));
        continue;
      }
      throw err;
    }
  }

  throw new Error(`[api] Max retries exceeded for ${method} ${path}`);
}

/**
 * Fetch a page of events with smart auth selection.
 */
export async function fetchEvents(
  cursor?: string,
  limit: number = 1000,
  extraParams?: Record<string, string>,
  authMethod?: AuthMethod
): Promise<{ events: EventsResponse; rateLimit: RateLimitInfo; authMethod: AuthMethod }> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  if (cursor) params.set("cursor", cursor);
  if (extraParams) {
    for (const [k, v] of Object.entries(extraParams)) {
      params.set(k, v);
    }
  }

  const result = await apiRequest(`/events?${params.toString()}`, { authMethod });

  if (result.statusCode >= 400) {
    throw new Error(`[api] Events request failed (${result.statusCode}): ${result.body.substring(0, 200)}`);
  }

  const events = JSON.parse(result.body) as EventsResponse;
  return { events, rateLimit: result.rateLimit, authMethod: result.authMethod };
}

/**
 * Probe an endpoint - returns response info without throwing.
 */
export async function probeEndpoint(
  path: string,
  options: { method?: string; acceptHeader?: string; authMethod?: AuthMethod } = {}
): Promise<{
  status: number;
  headers: Record<string, string | string[]>;
  body: string;
  contentType: string;
  ok: boolean;
}> {
  try {
    const result = await apiRequest(path, options);
    return {
      status: result.statusCode,
      headers: result.headers,
      body: result.body.substring(0, 5000),
      contentType: String(result.headers["content-type"] || ""),
      ok: result.statusCode >= 200 && result.statusCode < 300,
    };
  } catch {
    return {
      status: 0,
      headers: {},
      body: "",
      contentType: "",
      ok: false,
    };
  }
}

export function getApiBaseUrl(): string {
  return API_BASE_URL;
}

/**
 * Get the host base URL (without /api/v1) for accessing /internal/ paths.
 */
export function getHostBaseUrl(): string {
  return API_BASE_URL.replace(/\/api\/v1\/?$/, "");
}

export function getApiKey(): string {
  return API_KEY;
}

/**
 * Request stream access from the dashboard internal endpoint.
 * POST /internal/dashboard/stream-access with X-API-Key header
 * Returns { streamAccess: { endpoint: "URL", expiresIn: N } }
 */
export interface StreamAccessResult {
  endpoint: string;
  expiresIn: number;
  token: string | null;
  tokenHeader: string | null;
}

export async function requestStreamAccess(): Promise<StreamAccessResult | null> {
  const hostBase = getHostBaseUrl();
  const url = `${hostBase}/internal/dashboard/stream-access`;

  try {
    const { statusCode, body: bodyStream } = await undiciRequest(url, {
      method: "POST",
      headers: {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      dispatcher: agent,
      headersTimeout: 30_000,
      bodyTimeout: 30_000,
    });

    const bodyText = await bodyStream.text();

    if (statusCode >= 200 && statusCode < 300) {
      const parsed = JSON.parse(bodyText);
      if (parsed.streamAccess?.endpoint) {
        return {
          endpoint: parsed.streamAccess.endpoint,
          expiresIn: parsed.streamAccess.expiresIn || 300,
          token: parsed.streamAccess.token || null,
          tokenHeader: parsed.streamAccess.tokenHeader || null,
        };
      }
    }

    console.log(`[api] stream-access returned ${statusCode}: ${bodyText.substring(0, 200)}`);
    return null;
  } catch (err) {
    console.log(`[api] stream-access error: ${String(err).substring(0, 200)}`);
    return null;
  }
}

/**
 * Fetch from a streaming endpoint URL (returned by stream-access).
 * Uses cursor and since params for pagination.
 */
export async function fetchStream(
  streamUrl: string,
  cursor?: string,
  since?: string,
  token?: string | null,
  tokenHeader?: string | null
): Promise<{
  statusCode: number;
  body: string;
  headers: Record<string, string | string[]>;
}> {
  const url = streamUrl.startsWith("http") ? new URL(streamUrl) : new URL(streamUrl, getHostBaseUrl());
  if (cursor) url.searchParams.set("cursor", cursor);
  if (since) url.searchParams.set("since", since);

  const reqHeaders: Record<string, string> = {
    Accept: "application/json",
    "X-API-Key": API_KEY,
  };
  if (token && tokenHeader) {
    reqHeaders[tokenHeader] = token;
  }

  const { statusCode, headers: respHeaders, body: bodyStream } = await undiciRequest(url.toString(), {
    method: "GET",
    headers: reqHeaders,
    dispatcher: agent,
    headersTimeout: 30_000,
    bodyTimeout: 300_000, // Stream responses can be large
  });

  const rawHeaders: Record<string, string | string[]> = {};
  for (const k of Object.keys(respHeaders)) {
    const v = respHeaders[k];
    if (v !== undefined) rawHeaders[k] = v;
  }

  const bodyText = await bodyStream.text();
  return { statusCode, body: bodyText, headers: rawHeaders };
}

export function destroyAgent(): void {
  agent.destroy();
}
