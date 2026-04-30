/**
 * File-based brief store. One JSON file per brief in DATA_DIR.
 * Good enough for MVP. Swap for Postgres/Supabase later if needed.
 */
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });

function pathFor(id) {
  return path.join(DATA_DIR, id + '.json');
}

function save(brief) {
  fs.writeFileSync(pathFor(brief.id), JSON.stringify(brief, null, 2));
  return brief;
}

function get(id) {
  try {
    return JSON.parse(fs.readFileSync(pathFor(id), 'utf8'));
  } catch (e) {
    return null;
  }
}

function list() {
  const files = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.json'));
  const all = files.map(f => {
    try {
      return JSON.parse(fs.readFileSync(path.join(DATA_DIR, f), 'utf8'));
    } catch (e) {
      return null;
    }
  }).filter(Boolean);
  // newest first
  all.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  return all;
}

function update(id, patch) {
  const b = get(id);
  if (!b) return null;
  Object.assign(b, patch);
  return save(b);
}

function appendLog(id, msg, kind = 'info') {
  const b = get(id);
  if (!b) return;
  b.log = b.log || [];
  b.log.push({ ts: new Date().toISOString(), msg, kind });
  // Cap log to last 500 lines
  if (b.log.length > 500) b.log = b.log.slice(-500);
  save(b);
}

module.exports = { save, get, list, update, appendLog };
