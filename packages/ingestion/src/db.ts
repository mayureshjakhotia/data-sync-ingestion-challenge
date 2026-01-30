/**
 * Database layer optimized for high-throughput parallel inserts.
 * Stores only event IDs (minimal storage/IO) since we only need IDs for submission.
 */

import { Pool } from "pg";

const DATABASE_URL =
  process.env.DATABASE_URL ||
  "postgresql://postgres:postgres@postgres:5432/ingestion";

// Store full data or just IDs
const ID_ONLY = process.env.ID_ONLY !== "false"; // default true

let pool: Pool;

export function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: DATABASE_URL,
      max: 30, // high pool for parallel workers
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
    });
    // Set synchronous_commit = OFF on each new connection for speed
    pool.on("connect", (client) => {
      client.query("SET synchronous_commit = OFF").catch(() => {});
    });
  }
  return pool;
}

export async function initDb(): Promise<void> {
  const p = getPool();

  if (ID_ONLY) {
    // Minimal table - just event IDs, fillfactor=100 (no update room needed), no autovacuum
    await p.query(`
      CREATE TABLE IF NOT EXISTS ingested_events (
        id TEXT PRIMARY KEY
      ) WITH (fillfactor = 100, autovacuum_enabled = false);
    `);
  } else {
    await p.query(`
      CREATE TABLE IF NOT EXISTS ingested_events (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        ingested_at TIMESTAMPTZ DEFAULT NOW()
      ) WITH (fillfactor = 100, autovacuum_enabled = false);
    `);
  }

  await p.query(`
    CREATE TABLE IF NOT EXISTS ingestion_progress (
      id SERIAL PRIMARY KEY,
      cursor TEXT,
      events_count BIGINT DEFAULT 0,
      status TEXT DEFAULT 'running',
      strategy TEXT,
      metadata JSONB DEFAULT '{}',
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Add unique index on strategy for UPSERT support (idempotent)
  await p.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_progress_strategy ON ingestion_progress (strategy)
      WHERE status != 'completed';
  `);

  // Set unlogged for speed (no WAL overhead - we can re-ingest if crash)
  try {
    await p.query(`ALTER TABLE ingested_events SET UNLOGGED`);
  } catch {
    // Already unlogged or not supported
  }

  console.log(`[db] Tables initialized (ID_ONLY=${ID_ONLY})`);
}

/**
 * Batch insert event IDs using unnest() - single parameter, no 65535-param limit, much faster.
 * Uses ON CONFLICT DO NOTHING for dedup.
 */
export async function batchInsertIds(ids: string[]): Promise<number> {
  if (ids.length === 0) return 0;

  const p = getPool();
  // unnest accepts a single text[] parameter - no chunking needed
  const result = await p.query(
    `INSERT INTO ingested_events (id) SELECT unnest($1::text[]) ON CONFLICT (id) DO NOTHING`,
    [ids]
  );
  return result.rowCount ?? 0;
}

/**
 * Batch insert IDs with retry logic for transient DB errors.
 */
export async function batchInsertIdsWithRetry(ids: string[], maxRetries = 3): Promise<number> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await batchInsertIds(ids);
    } catch (err) {
      if (attempt >= maxRetries - 1) throw err;
      console.log(`[db] Insert retry ${attempt + 1}/${maxRetries}: ${String(err).substring(0, 100)}`);
      await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  return 0; // unreachable
}

/**
 * Batch insert events with full data (fallback).
 */
export async function batchInsertEvents(
  events: Array<{ id: string; data: Record<string, unknown> }>
): Promise<number> {
  if (events.length === 0) return 0;

  if (ID_ONLY) {
    return batchInsertIds(events.map((e) => e.id));
  }

  const p = getPool();
  const CHUNK = 500;
  let total = 0;

  for (let i = 0; i < events.length; i += CHUNK) {
    const chunk = events.slice(i, i + CHUNK);
    const values: unknown[] = [];
    const placeholders: string[] = [];
    for (let j = 0; j < chunk.length; j++) {
      const offset = j * 2;
      placeholders.push(`($${offset + 1}, $${offset + 2})`);
      values.push(chunk[j].id, JSON.stringify(chunk[j].data));
    }

    const result = await p.query(
      `INSERT INTO ingested_events (id, data) VALUES ${placeholders.join(", ")} ON CONFLICT (id) DO NOTHING`,
      values
    );
    total += result.rowCount ?? 0;
  }

  return total;
}

/**
 * Get ingested count. Uses estimated count for speed when count is large.
 */
export async function getIngestedCount(exact?: boolean): Promise<number> {
  const p = getPool();

  if (!exact) {
    // Fast estimated count from pg_class
    try {
      const est = await p.query(
        `SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'ingested_events'`
      );
      const estimate = Number(est.rows[0]?.estimate || 0);
      // Use estimate if > 10000 (accurate enough for progress tracking)
      if (estimate > 10000) return estimate;
    } catch {
      // Fall through to exact count
    }
  }

  const result = await p.query("SELECT COUNT(*)::int as count FROM ingested_events");
  return result.rows[0].count;
}

/**
 * Exact count - always accurate.
 */
export async function getExactCount(): Promise<number> {
  return getIngestedCount(true);
}

export async function getAllEventIds(): Promise<string[]> {
  const p = getPool();
  // No ORDER BY - we don't need sorted IDs, just all of them
  const result = await p.query("SELECT id FROM ingested_events");
  return result.rows.map((r: { id: string }) => r.id);
}

/**
 * Stream event IDs from DB using a server-side cursor to avoid OOM.
 * Calls `callback` with batches of IDs.
 */
export async function streamEventIds(
  callback: (ids: string[]) => Promise<void> | void,
  batchSize: number = 50000
): Promise<number> {
  const p = getPool();
  const client = await p.connect();
  let total = 0;
  try {
    await client.query("BEGIN");
    await client.query(
      `DECLARE id_cursor CURSOR FOR SELECT id FROM ingested_events`
    );
    let done = false;
    while (!done) {
      const result = await client.query(
        `FETCH ${batchSize} FROM id_cursor`
      );
      if (result.rows.length === 0) {
        done = true;
      } else {
        const ids = result.rows.map((r: { id: string }) => r.id);
        await callback(ids);
        total += ids.length;
      }
    }
    await client.query("CLOSE id_cursor");
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
  return total;
}

export async function saveProgress(
  cursor: string | null,
  eventsCount: number,
  status: string,
  strategy: string,
  metadata?: Record<string, unknown>
): Promise<void> {
  const p = getPool();
  await p.query(
    `INSERT INTO ingestion_progress (cursor, events_count, status, strategy, metadata, updated_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (strategy) WHERE status != 'completed'
     DO UPDATE SET cursor = $1, events_count = $2, status = $3, metadata = $5, updated_at = NOW()`,
    [cursor, eventsCount, status, strategy, JSON.stringify(metadata || {})]
  );
}

export async function getLastProgress(): Promise<{
  cursor: string | null;
  events_count: number;
  strategy: string;
  metadata: Record<string, unknown>;
} | null> {
  const p = getPool();
  const result = await p.query(
    `SELECT cursor, events_count, strategy, metadata
     FROM ingestion_progress
     WHERE status != 'completed'
     ORDER BY updated_at DESC
     LIMIT 1`
  );
  return result.rows[0] || null;
}

/**
 * Run ANALYZE to update statistics for accurate estimated counts.
 */
export async function analyzeTable(): Promise<void> {
  const p = getPool();
  await p.query("ANALYZE ingested_events");
}

export async function closeDb(): Promise<void> {
  if (pool) {
    await pool.end();
  }
}
