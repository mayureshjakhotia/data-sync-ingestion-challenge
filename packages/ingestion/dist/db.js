"use strict";
/**
 * Database layer optimized for high-throughput parallel inserts.
 * Stores only event IDs (minimal storage/IO) since we only need IDs for submission.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPool = getPool;
exports.initDb = initDb;
exports.batchInsertIds = batchInsertIds;
exports.batchInsertEvents = batchInsertEvents;
exports.getIngestedCount = getIngestedCount;
exports.getExactCount = getExactCount;
exports.getAllEventIds = getAllEventIds;
exports.saveProgress = saveProgress;
exports.getLastProgress = getLastProgress;
exports.analyzeTable = analyzeTable;
exports.closeDb = closeDb;
const pg_1 = require("pg");
const DATABASE_URL = process.env.DATABASE_URL ||
    "postgresql://postgres:postgres@postgres:5432/ingestion";
// Store full data or just IDs
const ID_ONLY = process.env.ID_ONLY !== "false"; // default true
let pool;
function getPool() {
    if (!pool) {
        pool = new pg_1.Pool({
            connectionString: DATABASE_URL,
            max: 30, // high pool for parallel workers
            idleTimeoutMillis: 30_000,
            connectionTimeoutMillis: 10_000,
        });
    }
    return pool;
}
async function initDb() {
    const p = getPool();
    if (ID_ONLY) {
        // Minimal table - just event IDs
        await p.query(`
      CREATE TABLE IF NOT EXISTS ingested_events (
        id TEXT PRIMARY KEY
      );
    `);
    }
    else {
        await p.query(`
      CREATE TABLE IF NOT EXISTS ingested_events (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        ingested_at TIMESTAMPTZ DEFAULT NOW()
      );
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
    // Set unlogged for speed (no WAL overhead - we can re-ingest if crash)
    try {
        await p.query(`ALTER TABLE ingested_events SET UNLOGGED`);
    }
    catch {
        // Already unlogged or not supported
    }
    console.log(`[db] Tables initialized (ID_ONLY=${ID_ONLY})`);
}
/**
 * Batch insert event IDs only - maximum speed.
 * Uses multi-row INSERT with ON CONFLICT DO NOTHING for dedup.
 */
async function batchInsertIds(ids) {
    if (ids.length === 0)
        return 0;
    const p = getPool();
    const CHUNK = 2000; // Postgres param limit is 65535, each row = 1 param
    let total = 0;
    for (let i = 0; i < ids.length; i += CHUNK) {
        const chunk = ids.slice(i, i + CHUNK);
        const placeholders = chunk.map((_, idx) => `($${idx + 1})`).join(",");
        const result = await p.query(`INSERT INTO ingested_events (id) VALUES ${placeholders} ON CONFLICT (id) DO NOTHING`, chunk);
        total += result.rowCount ?? 0;
    }
    return total;
}
/**
 * Batch insert events with full data (fallback).
 */
async function batchInsertEvents(events) {
    if (events.length === 0)
        return 0;
    if (ID_ONLY) {
        return batchInsertIds(events.map((e) => e.id));
    }
    const p = getPool();
    const CHUNK = 500;
    let total = 0;
    for (let i = 0; i < events.length; i += CHUNK) {
        const chunk = events.slice(i, i + CHUNK);
        const values = [];
        const placeholders = [];
        for (let j = 0; j < chunk.length; j++) {
            const offset = j * 2;
            placeholders.push(`($${offset + 1}, $${offset + 2})`);
            values.push(chunk[j].id, JSON.stringify(chunk[j].data));
        }
        const result = await p.query(`INSERT INTO ingested_events (id, data) VALUES ${placeholders.join(", ")} ON CONFLICT (id) DO NOTHING`, values);
        total += result.rowCount ?? 0;
    }
    return total;
}
/**
 * Get ingested count. Uses estimated count for speed when count is large.
 */
async function getIngestedCount(exact) {
    const p = getPool();
    if (!exact) {
        // Fast estimated count from pg_class
        try {
            const est = await p.query(`SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'ingested_events'`);
            const estimate = Number(est.rows[0]?.estimate || 0);
            // Use estimate if > 10000 (accurate enough for progress tracking)
            if (estimate > 10000)
                return estimate;
        }
        catch {
            // Fall through to exact count
        }
    }
    const result = await p.query("SELECT COUNT(*)::int as count FROM ingested_events");
    return result.rows[0].count;
}
/**
 * Exact count - always accurate.
 */
async function getExactCount() {
    return getIngestedCount(true);
}
async function getAllEventIds() {
    const p = getPool();
    const result = await p.query("SELECT id FROM ingested_events ORDER BY id");
    return result.rows.map((r) => r.id);
}
async function saveProgress(cursor, eventsCount, status, strategy, metadata) {
    const p = getPool();
    await p.query(`INSERT INTO ingestion_progress (cursor, events_count, status, strategy, metadata, updated_at)
     VALUES ($1, $2, $3, $4, $5, NOW())`, [cursor, eventsCount, status, strategy, JSON.stringify(metadata || {})]);
}
async function getLastProgress() {
    const p = getPool();
    const result = await p.query(`SELECT cursor, events_count, strategy, metadata
     FROM ingestion_progress
     WHERE status != 'completed'
     ORDER BY updated_at DESC
     LIMIT 1`);
    return result.rows[0] || null;
}
/**
 * Run ANALYZE to update statistics for accurate estimated counts.
 */
async function analyzeTable() {
    const p = getPool();
    await p.query("ANALYZE ingested_events");
}
async function closeDb() {
    if (pool) {
        await pool.end();
    }
}
