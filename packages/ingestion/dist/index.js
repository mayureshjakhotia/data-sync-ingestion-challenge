"use strict";
/**
 * DataSync Ingestion - Entry Point
 *
 * Orchestrates: DB init -> Discovery -> Parallel Ingestion -> Submission
 * Handles graceful shutdown on SIGTERM/SIGINT.
 */
Object.defineProperty(exports, "__esModule", { value: true });
const db_1 = require("./db");
const ingest_1 = require("./ingest");
const submit_1 = require("./submit");
const api_1 = require("./api");
let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown)
        return;
    isShuttingDown = true;
    console.log(`\n[main] ${signal} received. Graceful shutdown...`);
    (0, ingest_1.requestShutdown)();
    // Give workers 5s to finish current batches
    await new Promise((resolve) => setTimeout(resolve, 5000));
    try {
        const count = await (0, db_1.getIngestedCount)();
        await (0, db_1.saveProgress)(null, count, "interrupted", signal);
        console.log(`[main] Saved progress: ${count} events`);
    }
    catch {
        // DB may already be closed
    }
    try {
        (0, api_1.destroyAgent)();
        await (0, db_1.closeDb)();
    }
    catch {
        // ignore
    }
    process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
async function main() {
    const startTime = Date.now();
    console.log("==============================================");
    console.log("  DataSync Ingestion Service (Parallel)");
    console.log("==============================================");
    console.log(`  API: ${process.env.API_BASE_URL || "(default)"}`);
    console.log(`  Key: ${process.env.TARGET_API_KEY ? "***configured***" : "MISSING!"}`);
    console.log(`  Workers: parallel auto-detect`);
    console.log("==============================================\n");
    if (!process.env.TARGET_API_KEY) {
        console.error("[main] ERROR: TARGET_API_KEY environment variable is required");
        process.exit(1);
    }
    try {
        // Step 1: Initialize database
        console.log("[main] Initializing database...");
        await (0, db_1.initDb)();
        // Step 2: Check current state
        const existingCount = await (0, db_1.getIngestedCount)();
        console.log(`[main] Existing events in DB: ${existingCount}`);
        // Step 3: Run ingestion
        if (existingCount < 3_000_000) {
            console.log("[main] Starting parallel ingestion...");
            const ingestStart = Date.now();
            await (0, ingest_1.runIngestion)();
            const ingestTime = ((Date.now() - ingestStart) / 1000 / 60).toFixed(1);
            console.log(`[main] Ingestion completed in ${ingestTime} minutes`);
        }
        else {
            console.log("[main] Already have 3M+ events, skipping ingestion.");
        }
        // Step 4: Verify count
        const finalCount = await (0, db_1.getExactCount)();
        console.log(`[main] Final event count: ${finalCount}`);
        // Step 5: Submit if we have enough events
        if (finalCount >= 3_000_000 && process.env.AUTO_SUBMIT !== "false") {
            console.log("[main] Auto-submitting results...");
            await (0, submit_1.submitResults)();
        }
        else if (finalCount < 3_000_000) {
            console.log(`[main] Only ${finalCount} events ingested. Need 3,000,000 for submission.`);
        }
        const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        console.log(`\n[main] Total time: ${elapsed} minutes`);
        console.log("ingestion complete");
    }
    catch (err) {
        console.error("[main] Fatal error:", err);
        process.exit(1);
    }
    finally {
        (0, api_1.destroyAgent)();
        await (0, db_1.closeDb)();
    }
}
main();
