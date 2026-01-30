/**
 * DataSync Ingestion - Entry Point
 *
 * Orchestrates: DB init -> Discovery -> Parallel Ingestion -> Submission
 * Handles graceful shutdown on SIGTERM/SIGINT.
 */

import { initDb, getIngestedCount, getExactCount, closeDb, saveProgress } from "./db";
import { runIngestion, requestShutdown, closeWal } from "./ingest";
import { submitResults } from "./submit";
import { destroyAgent, apiRequest } from "./api";

let isShuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\n[main] ${signal} received. Graceful shutdown...`);

  requestShutdown();

  // Give workers 5s to finish current batches
  await new Promise((resolve) => setTimeout(resolve, 5000));

  try {
    const count = await getIngestedCount();
    await saveProgress(null, count, "interrupted", signal);
    console.log(`[main] Saved progress: ${count} events`);
  } catch {
    // DB may already be closed
  }

  try {
    closeWal();
    destroyAgent();
    await closeDb();
  } catch {
    // ignore
  }

  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

async function main(): Promise<void> {
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
    await initDb();

    // Step 2: Validate API key
    console.log("[main] Validating API key...");
    try {
      const testResult = await apiRequest("/events?limit=1");
      console.log(`[main] API key validated (status: ${testResult.statusCode})`);
    } catch (err) {
      console.error("[main] API key validation failed:", String(err).substring(0, 300));
      console.error("[main] Please check your TARGET_API_KEY environment variable.");
      process.exit(1);
    }

    // Step 3: Check current state
    const existingCount = await getIngestedCount();
    console.log(`[main] Existing events in DB: ${existingCount}`);

    // Step 4: Run ingestion
    if (existingCount < 3_000_000) {
      console.log("[main] Starting parallel ingestion...");
      const ingestStart = Date.now();
      await runIngestion();
      const ingestTime = ((Date.now() - ingestStart) / 1000 / 60).toFixed(1);
      console.log(`[main] Ingestion completed in ${ingestTime} minutes`);
    } else {
      console.log("[main] Already have 3M+ events, skipping ingestion.");
    }

    // Step 5: Verify count
    const finalCount = await getExactCount();
    console.log(`[main] Final event count: ${finalCount.toLocaleString()}`);

    // Step 6: Submit if we have enough events AND auto-submit is enabled
    if (finalCount >= 3_000_000) {
      if (process.env.AUTO_SUBMIT === "true") {
        console.log("[main] Auto-submitting results (3M+ events confirmed)...");
        await submitResults();
      } else {
        console.log("[main] Ingestion complete with 3M+ events. AUTO_SUBMIT is not enabled.");
        console.log("[main] To submit manually, run:");
        console.log(`[main]   docker exec assignment-postgres psql -U postgres -d ingestion -t -A -c "SELECT id FROM ingested_events" > event_ids.txt`);
        console.log(`[main]   curl -X POST -H "X-API-Key: $TARGET_API_KEY" -H "Content-Type: text/plain" --data-binary @event_ids.txt "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions?github_repo=REPO_URL"`);
      }
    } else {
      console.error(`[main] INSUFFICIENT EVENTS: ${finalCount.toLocaleString()}/3,000,000. Cannot submit.`);
    }

    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`\n[main] Total time: ${elapsed} minutes`);
    console.log("ingestion complete");
  } catch (err) {
    console.error("[main] Fatal error:", err);
    process.exit(1);
  } finally {
    closeWal();
    destroyAgent();
    await closeDb();
  }
}

main();
