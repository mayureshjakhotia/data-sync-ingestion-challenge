/**
 * Submission logic with safety guards.
 *
 * CRITICAL: We only get 5 submission attempts per API key. Each POST to /submissions
 * counts as one attempt regardless of success/failure. The code REFUSES to submit
 * unless the DB contains >= 3,000,000 unique event IDs.
 *
 * Flow:
 * 1. Verify count >= 3M (exact count, not estimated)
 * 2. Stream IDs from PostgreSQL to a temp file via server-side cursor (avoids OOM)
 * 3. POST the file content as text/plain to /submissions
 * 4. Only retry on network errors (connection reset, timeout) — NOT on HTTP responses
 *    since any HTTP response means the server received and counted the submission
 */

import * as fs from "fs";
import * as path from "path";
import { apiRequest, getApiKey } from "./api";
import { streamEventIds, getIngestedCount } from "./db";

const GITHUB_REPO = process.env.GITHUB_REPO || "";
const REQUIRED_COUNT = 3_000_000;
const TMP_IDS_FILE = path.join(process.env.WAL_DIR || "/tmp", "submit_ids.txt");

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Check remaining submissions before attempting.
 */
async function checkRemainingSubmissions(): Promise<number | null> {
  try {
    const result = await apiRequest("/submissions", { method: "GET" });
    if (result.statusCode === 200) {
      const parsed = JSON.parse(result.body);
      // Try to extract remaining submissions count
      const remaining = parsed.remainingSubmissions ?? parsed.remaining ?? null;
      if (remaining !== null) {
        console.log(`[submit] Remaining submissions: ${remaining}`);
        return Number(remaining);
      }
      // Log full response for inspection
      console.log(`[submit] GET /submissions response: ${JSON.stringify(parsed).substring(0, 500)}`);
    }
  } catch (err) {
    console.log(`[submit] Could not check remaining submissions: ${String(err).substring(0, 100)}`);
  }
  return null;
}

export async function submitResults(): Promise<void> {
  // Safety check 1: Exact count must be >= 3M
  const count = await getIngestedCount(true);
  console.log(`[submit] Total events in DB (exact count): ${count.toLocaleString()}`);

  if (count < REQUIRED_COUNT) {
    console.error(
      `[submit] REFUSING TO SUBMIT: Only ${count.toLocaleString()} events in DB. ` +
      `Need ${REQUIRED_COUNT.toLocaleString()}. We only get 5 submission attempts — cannot waste one.`
    );
    return;
  }

  // Safety check 2: Check remaining submissions if possible
  const remaining = await checkRemainingSubmissions();
  if (remaining !== null && remaining <= 0) {
    console.error("[submit] REFUSING TO SUBMIT: No remaining submission attempts!");
    return;
  }

  // Stream IDs to a temp file to avoid OOM
  console.log("[submit] Streaming event IDs to temp file...");
  const writeStream = fs.createWriteStream(TMP_IDS_FILE);
  let totalIds = 0;
  let isFirst = true;

  await streamEventIds(async (ids) => {
    const chunk = isFirst ? ids.join("\n") : "\n" + ids.join("\n");
    isFirst = false;
    writeStream.write(chunk);
    totalIds += ids.length;
    if (totalIds % 500000 === 0) {
      console.log(`[submit] Streamed ${totalIds.toLocaleString()} IDs to file...`);
    }
  });

  // Finish writing
  await new Promise<void>((resolve, reject) => {
    writeStream.end(() => resolve());
    writeStream.on("error", reject);
  });

  const stat = fs.statSync(TMP_IDS_FILE);
  console.log(`[submit] Got ${totalIds.toLocaleString()} event IDs (${(stat.size / 1024 / 1024).toFixed(1)} MB)`);

  // Safety check 3: Verify the file has the expected number of IDs
  if (totalIds < REQUIRED_COUNT) {
    console.error(
      `[submit] REFUSING TO SUBMIT: Only ${totalIds.toLocaleString()} IDs streamed to file ` +
      `(expected ${REQUIRED_COUNT.toLocaleString()}). DB count and stream count mismatch.`
    );
    try { fs.unlinkSync(TMP_IDS_FILE); } catch { /* ignore */ }
    return;
  }

  // Read the file back for submission
  const idsText = fs.readFileSync(TMP_IDS_FILE, "utf-8");

  const repoParam = GITHUB_REPO
    ? `?github_repo=${encodeURIComponent(GITHUB_REPO)}`
    : "";

  // SINGLE attempt — each POST counts against the 5-submission limit.
  // Only retry on network-level failures (connection refused, timeout, etc.)
  // where the server never received the request.
  const MAX_NETWORK_RETRIES = 2;

  for (let attempt = 0; attempt < MAX_NETWORK_RETRIES; attempt++) {
    try {
      console.log(`[submit] Submitting ${totalIds.toLocaleString()} IDs to API...`);
      if (GITHUB_REPO) {
        console.log(`[submit] GitHub repo: ${GITHUB_REPO}`);
      }

      const result = await apiRequest(`/submissions${repoParam}`, {
        method: "POST",
        headers: {
          "Content-Type": "text/plain",
        },
        body: idsText,
      });

      // We got an HTTP response — the server received it. Do NOT retry.
      let parsed: unknown;
      try {
        parsed = JSON.parse(result.body);
      } catch {
        parsed = result.body;
      }
      console.log("[submit] Submission response:", JSON.stringify(parsed, null, 2));

      if (result.statusCode >= 200 && result.statusCode < 300) {
        console.log("[submit] Submission successful!");
      } else {
        console.error(`[submit] Submission returned status ${result.statusCode}. This attempt has been consumed.`);
      }

      // Cleanup and return — do NOT retry regardless of status code
      try { fs.unlinkSync(TMP_IDS_FILE); } catch { /* ignore */ }
      return;
    } catch (err) {
      // Network-level error (connection refused, timeout, DNS failure)
      // The server may not have received the request, so retrying is safe
      console.error(`[submit] Network error (attempt ${attempt + 1}/${MAX_NETWORK_RETRIES}):`, String(err).substring(0, 200));

      if (attempt < MAX_NETWORK_RETRIES - 1) {
        const waitMs = 3000 * (attempt + 1);
        console.log(`[submit] Retrying in ${waitMs}ms (network error only — server likely didn't receive request)...`);
        await sleep(waitMs);
      }
    }
  }

  console.error("[submit] All network-level submission attempts failed. The submission was NOT consumed.");
  // Cleanup temp file
  try { fs.unlinkSync(TMP_IDS_FILE); } catch { /* ignore */ }
}
