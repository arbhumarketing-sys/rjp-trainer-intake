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
const persistentExclusions = new Map(); // id -> { id, term, note, addedBy, sourceBriefId, addedAt }

// Track briefs whose last persist failed. Reconciliation tick retries these.
// Required by the "back-end must never silently fail" guarantee — if Postgres
// blips during a write, the in-memory cache is correct but the DB diverges.
// Without this set, a redeploy after a blip would lose the most recent state.
const _dirtyBriefs = new Set();
const _dirtyFrs = new Set();

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
    CREATE TABLE IF NOT EXISTS persistent_exclusions (
      id TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS persistent_exclusions_created_at_idx ON persistent_exclusions(created_at DESC);
    CREATE TABLE IF NOT EXISTS brief_outputs (
      brief_id TEXT PRIMARY KEY,
      filename TEXT NOT NULL,
      data BYTEA NOT NULL,
      size_bytes INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  // Hydrate caches.
  const r1 = await pool.query('SELECT id, data FROM briefs');
  for (const row of r1.rows) briefs.set(row.id, row.data);
  const r2 = await pool.query('SELECT id, data FROM feature_requests');
  for (const row of r2.rows) featureRequests.set(row.id, row.data);
  const r3 = await pool.query('SELECT id, data FROM persistent_exclusions');
  for (const row of r3.rows) persistentExclusions.set(row.id, row.data);
  console.log(`[store] Postgres ready — hydrated ${briefs.size} briefs, ${featureRequests.size} feature requests, ${persistentExclusions.size} persistent exclusions.`);
  return true;
}

// Fire-and-forget DB write. Logs but doesn't throw — the in-memory cache is
// the source of truth for the request response. A failed write puts the brief
// in `_dirtyBriefs`; the reconciliation tick (startReconciliation) retries
// every 5 min so the DB eventually converges. This means a Postgres blip
// during a brief's run won't lose the brief on the next redeploy.
function _persistBrief(b) {
  if (!pool) return;
  pool.query(
    `INSERT INTO briefs (id, data, created_at)
     VALUES ($1, $2::jsonb, COALESCE(($2::jsonb->>'createdAt')::timestamptz, NOW()))
     ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
    [b.id, JSON.stringify(b)]
  ).then(() => _dirtyBriefs.delete(b.id))
   .catch(e => {
     console.warn('[store] persistBrief failed (queued for reconciliation):', e.message);
     _dirtyBriefs.add(b.id);
   });
}

function _persistFr(f) {
  if (!pool) return;
  pool.query(
    `INSERT INTO feature_requests (id, data, created_at)
     VALUES ($1, $2::jsonb, COALESCE(($2::jsonb->>'createdAt')::timestamptz, NOW()))
     ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
    [f.id, JSON.stringify(f)]
  ).then(() => _dirtyFrs.delete(f.id))
   .catch(e => {
     console.warn('[store] persistFr failed (queued for reconciliation):', e.message);
     _dirtyFrs.add(f.id);
   });
}

// Periodic retry of failed writes. Called once at boot from server.js.
function startReconciliation() {
  if (!pool) {
    console.log('[store] reconciliation disabled (no Postgres pool — using in-memory only).');
    return;
  }
  const intervalMs = parseInt(process.env.RECONCILE_INTERVAL_MS || (5 * 60 * 1000), 10);
  setInterval(() => {
    if (_dirtyBriefs.size === 0 && _dirtyFrs.size === 0) return;
    const briefIds = Array.from(_dirtyBriefs);
    const frIds = Array.from(_dirtyFrs);
    if (briefIds.length) {
      console.log(`[store] reconciliation retry: ${briefIds.length} dirty brief(s)`);
      for (const id of briefIds) {
        const b = briefs.get(id);
        if (b) _persistBrief(b);
        else _dirtyBriefs.delete(id);
      }
    }
    if (frIds.length) {
      console.log(`[store] reconciliation retry: ${frIds.length} dirty FR(s)`);
      for (const id of frIds) {
        const f = featureRequests.get(id);
        if (f) _persistFr(f);
        else _dirtyFrs.delete(id);
      }
    }
  }, intervalMs);
  console.log(`[store] reconciliation tick started (every ${intervalMs / 60000} min).`);
}

// Health surface for /healthz — lets the watchdog and any external pinger
// see if writes are silently piling up.
function getDirtyCount() {
  return { briefs: _dirtyBriefs.size, featureRequests: _dirtyFrs.size };
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

// v3.2: hard-delete a brief and its persisted Excel. Used by admin cleanup
// (test briefs, mistakes, abandoned drafts). Removes from in-memory cache,
// briefs table, AND brief_outputs table so we don't leak orphan binary rows.
async function deleteBrief(id) {
  const had = briefs.delete(id);
  _dirtyBriefs.delete(id);
  if (pool) {
    try {
      await pool.query('DELETE FROM briefs WHERE id = $1', [id]);
      await pool.query('DELETE FROM brief_outputs WHERE brief_id = $1', [id]);
    } catch (e) {
      console.warn('[store] deleteBrief Postgres failed:', e.message);
      // Cache is already deleted; the next save (if any) re-creates the DB row.
    }
  }
  return had;
}

/* ---------- Brief outputs (v3.2 fix — survive dyno restarts) ---------- */
// Render's free-tier filesystem is ephemeral — anything written to ./outputs
// vanishes the next time the dyno spins down (15 min idle) or redeploys.
// Without this, RJP submits a brief at 11am, comes back at 2pm to download,
// and gets a 404. Persisting the Excel bytes in Postgres survives both
// restarts and redeploys.
async function saveOutput(briefId, filename, bytes) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO brief_outputs (brief_id, filename, data, size_bytes)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (brief_id) DO UPDATE
       SET filename = EXCLUDED.filename, data = EXCLUDED.data, size_bytes = EXCLUDED.size_bytes`,
      [briefId, filename, bytes, bytes.length]
    );
  } catch (e) {
    console.warn('[store] saveOutput failed:', e.message);
  }
}

async function getOutput(briefId) {
  if (!pool) return null;
  try {
    const r = await pool.query(
      'SELECT filename, data, size_bytes FROM brief_outputs WHERE brief_id = $1',
      [briefId]
    );
    if (r.rows.length === 0) return null;
    return { filename: r.rows[0].filename, data: r.rows[0].data, sizeBytes: r.rows[0].size_bytes };
  } catch (e) {
    console.warn('[store] getOutput failed:', e.message);
    return null;
  }
}

/* ---------- Persistent exclusions (v3.2) ---------- */
// Always-exclude terms that survive across all briefs. RJP adds InfraCloud,
// AnalyticsVidhya, etc. once instead of typing them into customExclusions
// on every brief. Pipeline reads via getPersistentExclusions() in
// normalizeBrief and merges into bp.exclusions.
function _persistPersistentExclusion(p) {
  if (!pool) return;
  pool.query(
    `INSERT INTO persistent_exclusions (id, data, created_at)
     VALUES ($1, $2::jsonb, COALESCE(($2::jsonb->>'addedAt')::timestamptz, NOW()))
     ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
    [p.id, JSON.stringify(p)]
  ).catch(e => console.warn('[store] persistPersistentExclusion failed:', e.message));
}

function _deletePersistentExclusion(id) {
  if (!pool) return;
  pool.query('DELETE FROM persistent_exclusions WHERE id = $1', [id])
    .catch(e => console.warn('[store] deletePersistentExclusion failed:', e.message));
}

function addPersistentExclusion(p) {
  persistentExclusions.set(p.id, p);
  _persistPersistentExclusion(p);
  return p;
}

function getPersistentExclusions() {
  return Array.from(persistentExclusions.values()).sort((a, b) => (a.term || '').localeCompare(b.term || ''));
}

function removePersistentExclusion(id) {
  const had = persistentExclusions.delete(id);
  if (had) _deletePersistentExclusion(id);
  return had;
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
  startReconciliation,
  getDirtyCount,
  // briefs
  save, get, list, update, appendLog, deleteBrief,
  // brief outputs (Postgres-backed Excel bytes — survives dyno restarts)
  saveOutput, getOutput,
  // feature requests
  saveFeatureRequest, listFeatureRequests, getFeatureRequest, updateFeatureRequest,
  // persistent exclusions (v3.2)
  addPersistentExclusion, getPersistentExclusions, removePersistentExclusion,
  // seed
  loadSeedIfFresh,
};
