/**
 * File-based stores. Briefs + feature requests, one JSON file per record.
 *
 * v3.1 additions:
 *   - appendLog accepts a meta arg (per-stage timings, etc.)
 *   - Feature-requests collection (Form 2 from the WhatsApp thread).
 *   - Seed loader for backfilling Run01-05 test runs at server boot.
 */
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const FR_DIR = path.join(DATA_DIR, 'feature-requests');
const SEED_FILE = path.join(__dirname, 'seed-briefs.json');

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(FR_DIR, { recursive: true });

function pathFor(id) { return path.join(DATA_DIR, id + '.json'); }
function frPathFor(id) { return path.join(FR_DIR, id + '.json'); }

/* ---------- Briefs ---------- */
function save(brief) {
  fs.writeFileSync(pathFor(brief.id), JSON.stringify(brief, null, 2));
  return brief;
}

function get(id) {
  try { return JSON.parse(fs.readFileSync(pathFor(id), 'utf8')); }
  catch (e) { return null; }
}

function list() {
  const files = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.json'));
  const all = files.map(f => {
    try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, f), 'utf8')); }
    catch (e) { return null; }
  }).filter(Boolean);
  all.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  return all;
}

function update(id, patch) {
  const b = get(id);
  if (!b) return null;
  Object.assign(b, patch);
  return save(b);
}

function appendLog(id, msg, kind = 'info', meta = null) {
  const b = get(id);
  if (!b) return;
  b.log = b.log || [];
  b.log.push({ ts: new Date().toISOString(), msg, kind, ...(meta ? { meta } : {}) });
  if (b.log.length > 800) b.log = b.log.slice(-800);
  save(b);
}

/* ---------- Feature requests (Form 2) ---------- */
function saveFeatureRequest(fr) {
  fs.writeFileSync(frPathFor(fr.id), JSON.stringify(fr, null, 2));
  return fr;
}

function listFeatureRequests() {
  if (!fs.existsSync(FR_DIR)) return [];
  const files = fs.readdirSync(FR_DIR).filter(f => f.endsWith('.json'));
  const all = files.map(f => {
    try { return JSON.parse(fs.readFileSync(path.join(FR_DIR, f), 'utf8')); }
    catch (e) { return null; }
  }).filter(Boolean);
  all.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  return all;
}

function getFeatureRequest(id) {
  try { return JSON.parse(fs.readFileSync(frPathFor(id), 'utf8')); }
  catch (e) { return null; }
}

function updateFeatureRequest(id, patch) {
  const f = getFeatureRequest(id);
  if (!f) return null;
  Object.assign(f, patch);
  return saveFeatureRequest(f);
}

/* ---------- Seed loader (Run01-05 backfill) ---------- */
function loadSeedIfFresh() {
  // Loaded only if file exists AND there are no real briefs yet OR a special force flag.
  if (!fs.existsSync(SEED_FILE)) return 0;
  const force = process.env.RELOAD_SEED === '1';
  const existing = list();
  // If real briefs exist (non-Test prefix), don't seed unless forced.
  const hasReal = existing.some(b => !(b.title || '').startsWith('Test —'));
  if (hasReal && !force) {
    // Still check if seeded test runs are missing and add them
  }
  try {
    const seed = JSON.parse(fs.readFileSync(SEED_FILE, 'utf8'));
    const seeds = Array.isArray(seed) ? seed : (seed.briefs || []);
    let added = 0;
    for (const b of seeds) {
      if (!b.id) continue;
      if (!get(b.id) || force) {
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
  save, get, list, update, appendLog,
  saveFeatureRequest, listFeatureRequests, getFeatureRequest, updateFeatureRequest,
  loadSeedIfFresh,
};
