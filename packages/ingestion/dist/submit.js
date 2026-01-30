"use strict";
/**
 * Submission logic.
 * Extracts all event IDs from PostgreSQL and POSTs them to the submissions endpoint.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.submitResults = submitResults;
const api_1 = require("./api");
const db_1 = require("./db");
const GITHUB_REPO = process.env.GITHUB_REPO || "";
async function submitResults() {
    const count = await (0, db_1.getIngestedCount)(true);
    console.log(`[submit] Total events in DB: ${count}`);
    if (count < 3_000_000) {
        console.log(`[submit] WARNING: Only ${count} events. Expected 3,000,000. Submitting anyway.`);
    }
    console.log("[submit] Extracting all event IDs...");
    const ids = await (0, db_1.getAllEventIds)();
    console.log(`[submit] Got ${ids.length} event IDs`);
    const idsText = ids.join("\n");
    const repoParam = GITHUB_REPO
        ? `?github_repo=${encodeURIComponent(GITHUB_REPO)}`
        : "";
    console.log("[submit] Submitting to API...");
    const result = await (0, api_1.apiRequest)(`/submissions${repoParam}`, {
        method: "POST",
        headers: {
            "Content-Type": "text/plain",
        },
        body: idsText,
    });
    let parsed;
    try {
        parsed = JSON.parse(result.body);
    }
    catch {
        parsed = result.body;
    }
    console.log("[submit] Submission response:", JSON.stringify(parsed, null, 2));
    if (result.statusCode >= 200 && result.statusCode < 300) {
        console.log("[submit] Submission successful!");
    }
    else {
        console.error(`[submit] Submission failed (${result.statusCode}):`, parsed);
    }
}
