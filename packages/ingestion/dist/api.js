"use strict";
/**
 * API client with dual-auth support, independent rate limit tracking,
 * and connection pooling for maximum parallel throughput.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.getRateLimitState = getRateLimitState;
exports.pickAuthMethod = pickAuthMethod;
exports.waitForRateLimit = waitForRateLimit;
exports.apiRequest = apiRequest;
exports.fetchEvents = fetchEvents;
exports.probeEndpoint = probeEndpoint;
exports.getApiBaseUrl = getApiBaseUrl;
exports.getHostBaseUrl = getHostBaseUrl;
exports.getApiKey = getApiKey;
exports.destroyAgent = destroyAgent;
const undici_1 = require("undici");
const API_BASE_URL = process.env.API_BASE_URL ||
    "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1";
const API_KEY = process.env.TARGET_API_KEY || "";
// Connection pool agent for keep-alive
const agent = new undici_1.Agent({
    keepAliveTimeout: 30_000,
    keepAliveMaxTimeout: 60_000,
    pipelining: 1,
    connections: 50,
});
const rateLimits = {
    header: { remaining: 999, resetAt: 0 },
    query: { remaining: 999, resetAt: 0 },
};
function updateRateLimit(method, info) {
    rateLimits[method].remaining = info.remaining;
    // reset is seconds from epoch
    if (info.reset > 0) {
        rateLimits[method].resetAt = info.reset * 1000;
    }
    if (info.retryAfter) {
        rateLimits[method].resetAt = Date.now() + info.retryAfter * 1000;
    }
}
function getRateLimitState() {
    return rateLimits;
}
/**
 * Pick the best auth method - prefer the one with more remaining requests.
 * If one is exhausted, use the other.
 */
function pickAuthMethod(preferred) {
    const now = Date.now();
    const headerOk = rateLimits.header.remaining > 1 || now >= rateLimits.header.resetAt;
    const queryOk = rateLimits.query.remaining > 1 || now >= rateLimits.query.resetAt;
    if (preferred && ((preferred === "header" && headerOk) || (preferred === "query" && queryOk))) {
        return preferred;
    }
    if (headerOk && !queryOk)
        return "header";
    if (queryOk && !headerOk)
        return "query";
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
async function waitForRateLimit() {
    const now = Date.now();
    const headerOk = rateLimits.header.remaining > 1 || now >= rateLimits.header.resetAt;
    const queryOk = rateLimits.query.remaining > 1 || now >= rateLimits.query.resetAt;
    if (headerOk || queryOk)
        return;
    const waitUntil = Math.min(rateLimits.header.resetAt, rateLimits.query.resetAt);
    const waitMs = Math.max(waitUntil - now, 100);
    console.log(`[api] Both auth methods rate-limited. Waiting ${waitMs}ms`);
    await sleep(waitMs);
}
function parseRateLimitHeaders(headers) {
    const get = (key) => {
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
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
/**
 * Build URL with auth. For "header" auth, url stays clean. For "query" auth, append apiKey param.
 */
function buildUrl(path, authMethod) {
    let url = path.startsWith("http") ? path : `${API_BASE_URL}${path}`;
    if (authMethod === "query") {
        const sep = url.includes("?") ? "&" : "?";
        url += `${sep}apiKey=${encodeURIComponent(API_KEY)}`;
    }
    return url;
}
function buildHeaders(authMethod, extra, acceptHeader) {
    const h = {
        Accept: acceptHeader || "application/json",
        "Accept-Encoding": "gzip, deflate",
        ...extra,
    };
    if (authMethod === "header") {
        h["X-API-Key"] = API_KEY;
    }
    return h;
}
/**
 * Make an authenticated API request with rate limit handling, retries, and dual-auth.
 */
async function apiRequest(path, options = {}) {
    const method = options.method || "GET";
    const maxRetries = 5;
    let authMethod = options.authMethod || pickAuthMethod();
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        const url = buildUrl(path, authMethod);
        const reqHeaders = buildHeaders(authMethod, options.headers, options.acceptHeader);
        try {
            const { statusCode, headers: respHeaders, body: bodyStream } = await (0, undici_1.request)(url, {
                method: method,
                headers: reqHeaders,
                body: options.body,
                dispatcher: agent,
            });
            const rawHeaders = {};
            const hKeys = Object.keys(respHeaders);
            for (const k of hKeys) {
                const v = respHeaders[k];
                if (v !== undefined)
                    rawHeaders[k] = v;
            }
            const rateLimit = parseRateLimitHeaders(rawHeaders);
            updateRateLimit(authMethod, rateLimit);
            if (statusCode === 429) {
                // Rate limited - try switching auth method
                const otherMethod = authMethod === "header" ? "query" : "header";
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
            if (statusCode >= 500 && attempt < maxRetries - 1) {
                const waitMs = Math.min(1000 * Math.pow(2, attempt), 10000);
                await sleep(waitMs);
                continue;
            }
            return { statusCode, headers: rawHeaders, body: bodyText, rateLimit, authMethod };
        }
        catch (err) {
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
async function fetchEvents(cursor, limit = 1000, extraParams, authMethod) {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    if (cursor)
        params.set("cursor", cursor);
    if (extraParams) {
        for (const [k, v] of Object.entries(extraParams)) {
            params.set(k, v);
        }
    }
    const result = await apiRequest(`/events?${params.toString()}`, { authMethod });
    if (result.statusCode >= 400) {
        throw new Error(`[api] Events request failed (${result.statusCode}): ${result.body.substring(0, 200)}`);
    }
    const events = JSON.parse(result.body);
    return { events, rateLimit: result.rateLimit, authMethod: result.authMethod };
}
/**
 * Probe an endpoint - returns response info without throwing.
 */
async function probeEndpoint(path, options = {}) {
    try {
        const result = await apiRequest(path, options);
        return {
            status: result.statusCode,
            headers: result.headers,
            body: result.body.substring(0, 5000),
            contentType: String(result.headers["content-type"] || ""),
            ok: result.statusCode >= 200 && result.statusCode < 300,
        };
    }
    catch {
        return {
            status: 0,
            headers: {},
            body: "",
            contentType: "",
            ok: false,
        };
    }
}
function getApiBaseUrl() {
    return API_BASE_URL;
}
/**
 * Get the host base URL (without /api/v1) for accessing /internal/ paths.
 */
function getHostBaseUrl() {
    return API_BASE_URL.replace(/\/api\/v1\/?$/, "");
}
function getApiKey() {
    return API_KEY;
}
function destroyAgent() {
    agent.destroy();
}
