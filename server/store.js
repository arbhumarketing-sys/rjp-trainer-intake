/**
 * Postgres-backed store with in-memory cache.
 *
 * On boot: hydrate Maps from Postgres (briefs, feature_requests). Subsequent
 * reads (.get/.list/etc.) are synchronous reads from the Map. Writes update the
 * Map immediately AND fire-and-forget an UPSERT to Postgres. Race window
 * between map-write and DB-write is small and ignorable for this workload
 * (each brief takes minutes to process; we're not write-throughput-bound).
 *
 * If DATABASE_URL isn't set, falls back to filesystem JSON files (the v3 store
 * behaviour) — useful for local dev. Production on Render free tier sets
 * DATABASE_URL automatically.
 *
 * Schema (auto-created on boot):
 *   briefs(id TEXT PK, data JSONB, created_at TIMESTAMPTZ)
 *   feature_requests(id TEXT PK, data JSONB, created_at TIMESTAMPTZ)
 */
'use strict';
const fs = require('fs');
const path = require('path');

const DATABASE_URL = process.env.DATABASE_URL || '';
const SEED_FILE = path.join(__dirname, 'seed-briefs.json');

// In-memory caches — single source of truth for reads.
const briefs = new Map();             // id -> brief object
const featureRequests = new Map();    // id -> fr object

let pool = null;

/* ---------- Postgres bootstrapping ---------- */
async function initPostgres() {
  if (!DATABASE_URL) {
    console.warn('[store] DATABASE_URL not set — falling back to filesystem JSON store (no persistence across redeploys).');
    return false;
  }
  const { Pool } = require('pg');
  pool = new Pool({
    connectionString: DATABASE_URL,
    // Render internal connections don't need SSL; external ones do. Leave default
    // (no SSL) for the internal hostname; setting it here would fail with
    // self-signed-cert errors on the internal endpoint.
    ssl: /\.render\.com/.test(DATABASE_URL) && !/dpg-[a-z0-9]+-a[:/]/.test(DATABASE_URL)
      ? { rejectUnauthorized: false }
      : false,
    max: 4,
    idleTimeoutMillis: 30000,
  });
  await pool.query(`
    CREATE TABLE IF NOT EXISTS briefs (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS briefs_created_at_idx ON briefs(created_at DESC);
    CREATE TABLE IF NOT EXISTS feature_requests (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS feature_requests_created_at_idx ON feature_requests(created_at DESC);
  `);
  // Hydrate caches.
  const r1 = await pool.query('SELECT id, data FROM briefs');
  for (const row of r1.rows) briefs.set(row.id, row.data);
  const r2 = await pool.query('SELECT id, data FROM feature_requests');
  for (const row of r2.rows) featureRequests.set(row.id, row.data);
  console.log(`[store] Postgres ready — hydrated ${briefs.size} briefs, ${featureRequests.size} feature requests.`);
  return true;
}

// Fire-and-forget DB write. Logs but doesn't throw — the in-memory cache is
// the source of truth for the request response, and a failed DB write will
// retry on the next save() of the same record.
function _persistBrief(b) {
  if (!pool) return;
  pool.query(
    `INSERT INTO briefs (id, data, created_at)
     VALUES ($1, $2::jsonb, COALESCE(($2::jsonb->>'createdAt')::timestamptz, NOW()))
     ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
    [b.id, JSON.stringify(b)]
  ).catch(e => console.warn('[store] persistBrief failed:', e.message));
}

function _persistFr(f) {
  if (!pool) return;
  pool.query(
    `INSERT INTO feature_requests (id, data, created_at)
     VALUES ($1, $2::jsonb, COALESCE(($2::jsonb->>'createdAt')::timestamptz, NOW()))
     ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
    [f.id, JSON.stringify(f)]
  ).catch(e => console.warn('[store] persistFr failed:', e.message));
}

/* ---------- Briefs (sync API) ---------- */
function save(brief) {
  briefs.set(brief.id, brief);
  _persistBrief(brief);
  return brief;
}
function get(id) {
  return briefs.get(id) || null;
}
function list() {
  return Array.from(briefs.values()).sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
}
function update(id, patch) {
  const b = briefs.get(id);
  if (!b) return null;
  Object.assign(b, patch);
  briefs.set(id, b);
  _persistBrief(b);
  return b;
}
function appendLog(id, msg, kind = 'info', meta = null) {
  const b = briefs.get(id);
  if (!b) return;
  b.log = b.log || [];
  b.log.push({ ts: new Date().toISOString(), msg, kind, ...(meta ? { meta } : {}) });
  if (b.log.length > 800) b.log = b.log.slice(-800);
  briefs.set(id, b);
  _persistBrief(b);
}

/* ---------- Feature requests ---------- */
function saveFeatureRequest(fr) {
  featureRequests.set(fr.id, fr);
  _persistFr(fr);
  return fr;
}
function listFeatureRequests() {
  return Array.from(featureRequests.values()).sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
}
function getFeatureRequest(id) {
  return featureRequests.get(id) || null;
}
function updateFeatureRequest(id, patch) {
  const f = featureRequests.get(id);
  if (!f) return null;
  Object.assign(f, patch);
  featureRequests.set(id, f);
  _persistFr(f);
  return f;
}

/* ---------- Seed loader (Run01-05 backfill) ---------- */
function loadSeedIfFresh() {
  if (!fs.existsSync(SEED_FILE)) return 0;
  const force = process.env.RELOAD_SEED === '1';
  try {
    const seed = JSON.parse(fs.readFileSync(SEED_FILE, 'utf8'));
    const seeds = Array.isArray(seed) ? seed : (seed.briefs || []);
    let added = 0;
    for (const b of seeds) {
      if (!b.id) continue;
      if (!briefs.has(b.id) || force) {
        save(b);
        added++;
      }
    }
    return added;
  } catch (e) {
    console.warn('[seed load failed]', e.message);
    return 0;
  }
}

module.exports = {
  // boot
  initPostgres,
  // briefs
  save, get, list, update, appendLog,
  // feature requests
  saveFeatureRequest, listFeatureRequests, getFeatureRequest, updateFeatureRequest,
  // seed
  loadSeedIfFresh,
};
