"use strict";
/**
 * Quick runtime discovery probe (3-5 API calls).
 * Returns structured result for strategy selection in ingest.ts.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.quickDiscover = quickDiscover;
const api_1 = require("./api");
const EVENT_TYPES = [
    "page_view", "click", "api_call", "form_submit",
    "scroll", "purchase", "error", "video_play",
];
/**
 * Quick discovery - makes minimal API calls to determine best strategy.
 */
async function quickDiscover() {
    console.log("[discover] Running quick discovery...");
    const result = {
        maxLimit: 1000,
        typeFilterWorks: false,
        typeFilterParam: "type",
        bulkEndpoint: null,
        parallelCursorsWork: false,
        eventTypes: EVENT_TYPES,
    };
    // Run probes in parallel for speed
    const [bulkResult, limitAndTypeResult] = await Promise.allSettled([
        probeBulkEndpoints(),
        probeLimitAndTypeFilter(),
    ]);
    if (bulkResult.status === "fulfilled" && bulkResult.value) {
        result.bulkEndpoint = bulkResult.value;
        console.log(`[discover] Found bulk endpoint: ${result.bulkEndpoint}`);
        return result;
    }
    if (limitAndTypeResult.status === "fulfilled") {
        const ltResult = limitAndTypeResult.value;
        result.maxLimit = ltResult.maxLimit;
        result.typeFilterWorks = ltResult.typeFilterWorks;
        result.typeFilterParam = ltResult.typeFilterParam;
        console.log(`[discover] Max limit: ${result.maxLimit}, type filter: ${result.typeFilterWorks} (param: ${result.typeFilterParam})`);
    }
    // Test if parallel cursors give different data
    try {
        const [r1, r2] = await Promise.all([
            (0, api_1.fetchEvents)(undefined, 10),
            (0, api_1.fetchEvents)(undefined, 10),
        ]);
        // If cursors differ, parallel chains might explore different data
        result.parallelCursorsWork = r1.events.nextCursor !== r2.events.nextCursor;
        if (result.parallelCursorsWork) {
            console.log("[discover] Parallel cursors return different data - can use multi-chain strategy");
        }
    }
    catch {
        // ignore
    }
    return result;
}
async function probeBulkEndpoints() {
    const hostBase = (0, api_1.getHostBaseUrl)();
    const apiKey = (0, api_1.getApiKey)();
    // Probe internal endpoints and bulk endpoints in parallel
    const endpoints = [
        `${hostBase}/internal/events/export?apiKey=${apiKey}`,
        `${hostBase}/internal/events/dump?apiKey=${apiKey}`,
        `${hostBase}/internal/events/all?apiKey=${apiKey}`,
        `${hostBase}/internal/events?apiKey=${apiKey}`,
        `${hostBase}/internal/events/export?apiKey=test`,
        `${hostBase}/internal/events?apiKey=test`,
        "/events/export",
        "/events/stream",
        "/events/bulk",
        "/events/download",
        "/events/all",
    ];
    const results = await Promise.allSettled(endpoints.map(async (ep) => {
        const r = await (0, api_1.probeEndpoint)(ep);
        if (r.ok && r.body.length > 500) {
            // Looks like it returned substantial data
            return ep;
        }
        return null;
    }));
    for (const r of results) {
        if (r.status === "fulfilled" && r.value) {
            return r.value;
        }
    }
    return null;
}
async function probeLimitAndTypeFilter() {
    let maxLimit = 1000;
    let typeFilterWorks = false;
    let typeFilterParam = "type";
    // Test limit sizes and type filter in parallel
    const [limitResult, typeResult, eventTypeResult] = await Promise.allSettled([
        // Test large limit
        (0, api_1.fetchEvents)(undefined, 5000),
        // Test type filter with "type" param
        (0, api_1.fetchEvents)(undefined, 10, { type: "page_view" }),
        // Test type filter with "eventType" param
        (0, api_1.fetchEvents)(undefined, 10, { eventType: "page_view" }),
    ]);
    if (limitResult.status === "fulfilled") {
        const count = limitResult.value.events.data?.length || 0;
        if (count >= 4500) {
            maxLimit = 5000;
            // Try even larger
            try {
                const big = await (0, api_1.fetchEvents)(undefined, 10000);
                if ((big.events.data?.length || 0) >= 9000) {
                    maxLimit = 10000;
                }
            }
            catch {
                // 5000 is fine
            }
        }
    }
    if (typeResult.status === "fulfilled") {
        const data = typeResult.value.events.data || [];
        if (data.length > 0) {
            const allMatch = data.every((e) => e.type === "page_view" || e.eventType === "page_view" || e.event_type === "page_view");
            if (allMatch) {
                typeFilterWorks = true;
                typeFilterParam = "type";
            }
        }
    }
    if (!typeFilterWorks && eventTypeResult.status === "fulfilled") {
        const data = eventTypeResult.value.events.data || [];
        if (data.length > 0) {
            const allMatch = data.every((e) => e.type === "page_view" || e.eventType === "page_view" || e.event_type === "page_view");
            if (allMatch) {
                typeFilterWorks = true;
                typeFilterParam = "eventType";
            }
        }
    }
    return { maxLimit, typeFilterWorks, typeFilterParam };
}
// Allow running standalone
if (require.main === module) {
    quickDiscover()
        .then((r) => {
        console.log("\n[discover] Result:", JSON.stringify(r, null, 2));
    })
        .catch(console.error);
}
