/**
 * RJP Sourcing Portal — Express server v3.2.
 *
 * Routes:
 *   POST /api/auth/login           { password } -> { token, user }
 *   GET  /api/briefs               (auth) list
 *   POST /api/briefs               (auth) create + run pipeline
 *   GET  /api/briefs/:id           (auth) detail
 *   POST /api/briefs/:id/retry     (auth) re-run pipeline (no changes)
 *   POST /api/briefs/:id/preview   (auth) pre-flight: 1 query → 5 samples
 *   POST /api/briefs/:id/feedback  (auth) Form 1 — re-run with feedback applied as steering
 *   POST /api/briefs/:id/parse     (auth) Free-text intake parser → structured fields
 *   GET  /api/briefs/:id/output    (auth) download xlsx
 *   POST /api/feature-requests     (auth) Form 2 — submit feature suggestion
 *   GET  /api/feature-requests     (auth) admin list
 *   PATCH /api/feature-requests/:id (auth) approve/reject
 *   GET  /healthz                  liveness probe
 */
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const auth = require('./auth');
const store = require('./store');
const { runPipeline, runWithFeedback, startWatchdog, OUTPUT_DIR, DEFAULT_BIGFIRM_EXCLUSIONS } = require('./pipeline');

let Anthropic = null;
try { Anthropic = require('@anthropic-ai/sdk').default || require('@anthropic-ai/sdk'); } catch (_) {}

const _USE_CLI = process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true';
let ClaudeCliClient = null;
if (_USE_CLI) {
  try { ClaudeCliClient = require('./anthropic-claude-cli').ClaudeCliClient; } catch (_) {}
}
function makeLlmClient() {
  if (_USE_CLI) return ClaudeCliClient ? new ClaudeCliClient() : null;
  if (!Anthropic || !process.env.ANTHROPIC_API_KEY) return null;
  return new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
}

const PORT = parseInt(process.env.PORT || '3000', 10);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const ALLOW_ANY_ORIGIN = ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.length === 0;

const app = express();
app.use(express.json({ limit: '512kb' }));
app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);
    if (ALLOW_ANY_ORIGIN) return cb(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error('Origin not allowed: ' + origin));
  },
  credentials: false,
}));

/* Liveness — extended with dirty-write count and uptime so the keep-warm
   pinger and any external monitor can detect silent persistence drift. */
const _bootedAt = Date.now();
app.get('/healthz', (req, res) => {
  const dirty = (typeof store.getDirtyCount === 'function') ? store.getDirtyCount() : { briefs: 0, featureRequests: 0 };
  res.json({
    ok: true,
    ts: new Date().toISOString(),
    version: '3.2',
    uptimeSec: Math.round((Date.now() - _bootedAt) / 1000),
    storage: process.env.DATABASE_URL ? 'postgres' : 'filesystem',
    dirty,
    llm: process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true' ? 'claude-cli' : (process.env.ANTHROPIC_API_KEY ? 'api' : 'disabled'),
  });
});

/* Auth */
app.post('/api/auth/login', (req, res) => {
  const result = auth.login(req.body && req.body.password);
  if (!result) return res.status(401).send('Wrong password');
  res.json(result);
});

/* ---------- Briefs ---------- */
app.get('/api/briefs', auth.requireAuth, (req, res) => {
  res.json({ briefs: store.list(), defaults: { bigFirmExclusions: DEFAULT_BIGFIRM_EXCLUSIONS } });
});

function arrify(x) {
  if (!x) return [];
  if (Array.isArray(x)) return x.map(s => String(s).trim()).filter(Boolean);
  if (typeof x === 'string') return x.split(/[\n,]/).map(s => s.trim()).filter(Boolean);
  return [];
}

function buildBriefFromRequest(body, user, idOverride) {
  if (!body.title || (!body.keywords && (!Array.isArray(body.roles) || !body.roles.length))) {
    return { error: 'title + (keywords or roles[]) required' };
  }
  const id = idOverride || ('b_' + crypto.randomBytes(5).toString('hex') + '_' + Date.now().toString(36));
  const brief = {
    id,
    title: String(body.title).slice(0, 200),
    domain: String(body.domain || '').slice(0, 100),
    geo: String(body.geo || 'India').slice(0, 100),
    deadline: body.deadline || '',
    outputFormat: ['xlsx', 'pdf', 'both', 'sheet'].includes(body.outputFormat) ? body.outputFormat : 'xlsx',
    roles: (body.roles || []).slice(0, 25).map(r => ({
      title: String(r.title || '').slice(0, 200),
      skill: String(r.skill || '').slice(0, 200),
      bucket: String(r.bucket || '').slice(0, 100),
      count: Math.min(parseInt(r.count, 10) || 1, 200),
    })),
    // v3.1 net-new
    keywords: arrify(body.keywords).slice(0, 12),
    must: arrify(body.must).slice(0, 8),
    should: arrify(body.should).slice(0, 8),
    mustNot: arrify(body.mustNot).slice(0, 8),
    clientCompany: String(body.clientCompany || '').slice(0, 100),
    clientPrincipal: String(body.clientPrincipal || '').slice(0, 100),
    customExclusions: arrify(body.customExclusions).slice(0, 20),
    searchMode: body.searchMode === 'niche' ? 'niche' : 'std',
    advanced: typeof body.advanced === 'object' ? body.advanced : {},
    operator: typeof body.operator === 'object' ? body.operator : null,
    steering: String(body.steering || '').slice(0, 4000),
    status: 'queued',
    log: [],
    counts: {},
    createdAt: new Date().toISOString(),
    submittedBy: (user && user.team) || 'rjp-infotek',
  };
  return { brief };
}

app.post('/api/briefs', auth.requireAuth, (req, res) => {
  const body = req.body || {};
  const r = buildBriefFromRequest(body, req.user);
  if (r.error) return res.status(400).json({ error: r.error });
  store.save(r.brief);
  setImmediate(() => runPipeline(r.brief.id));
  res.json({ brief: r.brief });
});

app.get('/api/briefs/:id', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  res.json({ brief: b });
});

app.post('/api/briefs/:id/retry', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  store.update(req.params.id, {
    status: 'queued', error: null,
    log: (b.log || []).concat([{ ts: new Date().toISOString(), msg: 'Retry requested', kind: 'info' }]),
  });
  setImmediate(() => runPipeline(req.params.id));
  res.json({ ok: true });
});

/* Pre-flight preview — 1 query, 5 samples (Pre-flight wow feature) */
app.post('/api/briefs/:id/preview', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  store.update(req.params.id, {
    status: 'queued', error: null,
    log: (b.log || []).concat([{ ts: new Date().toISOString(), msg: 'Pre-flight preview requested', kind: 'info' }]),
  });
  setImmediate(() => runPipeline(req.params.id, { preview: true }));
  res.json({ ok: true, mode: 'preview' });
});

/* Form 1 — re-run with feedback (free text becomes new steering) */
app.post('/api/briefs/:id/feedback', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const text = String((req.body && req.body.feedback) || '').slice(0, 4000).trim();
  if (!text) return res.status(400).json({ error: 'feedback text required' });
  setImmediate(() => runWithFeedback(req.params.id, text));
  res.json({ ok: true, message: 'Feedback applied — re-running' });
});

/* Free-text intake parser — Claude turns one-liner into structured brief */
app.post('/api/briefs/parse', auth.requireAuth, async (req, res) => {
  const text = String((req.body && req.body.text) || '').slice(0, 2000).trim();
  if (!text) return res.status(400).json({ error: 'text required' });
  const client = makeLlmClient();
  if (!client) {
    return res.status(503).json({ error: 'Free-text parsing requires ANTHROPIC_API_KEY or ANTHROPIC_VIA_CLAUDE_CLI=true (none set)' });
  }
  try {
    const sys = `You parse one-line trainer-sourcing briefs into structured JSON. Output ONLY valid JSON with these keys: title (string), keywords (array), must (array), should (array), mustNot (array), clientCompany (string), customExclusions (array), searchMode ("std"|"niche"), deadline (string), notes (string).`;
    const user = `Parse this brief: "${text}"\nReturn the JSON only.`;
    const resp = await client.messages.create({
      model: process.env.CLAUDE_MODEL_FAST || 'claude-haiku-4-5-20251001',
      max_tokens: 600,
      system: sys,
      messages: [{ role: 'user', content: user }],
    });
    const out = (resp.content[0] && resp.content[0].text) || '';
    let parsed = {};
    try {
      const m = out.match(/\{[\s\S]*\}/);
      if (m) parsed = JSON.parse(m[0]);
    } catch (_) {}
    res.json({ parsed, raw: out });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/briefs/:id/output', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).send('not found');
  if (!b.outputFile) return res.status(409).send('output not ready');
  // Defence in depth: pipeline already stores `path.basename(file)` so b.outputFile
  // is a leaf filename, but verify the resolved path lives under OUTPUT_DIR so a
  // future bug (or hand-edited DB row) can't read /etc/passwd.
  const resolved = path.resolve(OUTPUT_DIR, b.outputFile);
  const outRoot = path.resolve(OUTPUT_DIR) + path.sep;
  if (!resolved.startsWith(outRoot) || !fs.existsSync(resolved)) {
    return res.status(404).send('file missing');
  }
  const safeTitle = (b.title || 'sourcing-output').replace(/[^a-zA-Z0-9-_ ]/g, '_').slice(0, 80);
  res.setHeader('Content-Disposition', `attachment; filename="${safeTitle} - ${b.id}.xlsx"`);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  fs.createReadStream(resolved).pipe(res);
});

/* ---------- Feature requests (Form 2 — admin dashboard only, no push) ---------- */
app.post('/api/feature-requests', auth.requireAuth, (req, res) => {
  const body = req.body || {};
  const text = String(body.text || '').slice(0, 4000).trim();
  if (!text) return res.status(400).json({ error: 'text required' });
  const fr = {
    id: 'fr_' + crypto.randomBytes(4).toString('hex') + '_' + Date.now().toString(36),
    text,
    submittedBy: String(body.submittedBy || (req.user && req.user.team) || 'rjp-infotek').slice(0, 100),
    contextBriefId: String(body.contextBriefId || '').slice(0, 80),
    status: 'pending',
    createdAt: new Date().toISOString(),
  };
  store.saveFeatureRequest(fr);
  res.json({ featureRequest: fr });
});

app.get('/api/feature-requests', auth.requireAuth, (req, res) => {
  res.json({ featureRequests: store.listFeatureRequests() });
});

app.patch('/api/feature-requests/:id', auth.requireAuth, (req, res) => {
  const id = req.params.id;
  const body = req.body || {};
  const valid = ['pending', 'approved', 'rejected', 'in-progress', 'shipped'];
  if (body.status !== undefined && !valid.includes(body.status)) {
    return res.status(400).json({ error: `invalid status (allowed: ${valid.join(', ')})` });
  }
  const patch = {};
  if (body.status) patch.status = body.status;
  if (typeof body.adminNote === 'string') patch.adminNote = body.adminNote.slice(0, 2000);
  patch.updatedAt = new Date().toISOString();
  const updated = store.updateFeatureRequest(id, patch);
  if (!updated) return res.status(404).json({ error: 'not found' });
  res.json({ featureRequest: updated });
});

/* ---------- Persistent exclusions (v3.2) ----------
   GET  /api/persistent-exclusions
   POST /api/persistent-exclusions    { term, note?, sourceBriefId? }
   DELETE /api/persistent-exclusions/:id

   These are always-exclude terms applied to every brief on top of the
   default big-firm list. Listed in admin tab; pipeline merges them into
   bp.exclusions during normalizeBrief. */
app.get('/api/persistent-exclusions', auth.requireAuth, (req, res) => {
  res.json({ persistentExclusions: store.getPersistentExclusions() });
});

app.post('/api/persistent-exclusions', auth.requireAuth, (req, res) => {
  const body = req.body || {};
  const term = String(body.term || '').trim();
  if (!term || term.length > 80) return res.status(400).json({ error: 'term required (1-80 chars)' });
  // Reject dupes (case-insensitive) so the list doesn't bloat
  const existing = store.getPersistentExclusions().find(p => (p.term || '').toLowerCase() === term.toLowerCase());
  if (existing) return res.status(409).json({ error: 'term already in persistent-exclusion list', existing });
  const p = {
    id: 'pex_' + crypto.randomBytes(4).toString('hex') + '_' + Date.now().toString(36),
    term,
    note: String(body.note || '').slice(0, 400),
    sourceBriefId: String(body.sourceBriefId || '').slice(0, 80),
    addedBy: String(body.addedBy || (req.user && req.user.team) || 'rjp-infotek').slice(0, 100),
    addedAt: new Date().toISOString(),
  };
  store.addPersistentExclusion(p);
  res.json({ persistentExclusion: p });
});

app.delete('/api/persistent-exclusions/:id', auth.requireAuth, (req, res) => {
  const ok = store.removePersistentExclusion(req.params.id);
  if (!ok) return res.status(404).json({ error: 'not found' });
  res.json({ ok: true });
});

/* Static frontend */
const FRONTEND_DIR = path.resolve(__dirname, '..', 'frontend');
if (fs.existsSync(path.join(FRONTEND_DIR, 'index.html'))) {
  app.use(express.static(FRONTEND_DIR));
  app.get('/', (req, res) => res.sendFile(path.join(FRONTEND_DIR, 'index.html')));
}

app.use((err, req, res, next) => {
  // Distinguish body-parser errors so clients get the right status code.
  if (err && err.type === 'entity.parse.failed') return res.status(400).json({ error: 'Invalid JSON body' });
  if (err && err.type === 'entity.too.large')   return res.status(413).json({ error: 'Body too large (limit 512kb)' });
  console.error('[server error]', err);
  res.status(500).json({ error: err.message });
});

/* Boot — hydrate Postgres cache BEFORE seeding and BEFORE app.listen so the
   first request never sees an empty store. If DATABASE_URL is missing the
   store falls back to filesystem JSON (no persistence), useful for local dev.
   After listen, start the watchdog (catches stuck briefs) and the
   reconciliation tick (retries failed Postgres writes). Both are part of the
   "back-end must never silently fail" guarantee. */
(async () => {
  await store.initPostgres();
  const seedAdded = store.loadSeedIfFresh();
  if (seedAdded) console.log(`[boot] Loaded ${seedAdded} seed briefs (Run01-05 test backfill)`);
  app.listen(PORT, () => {
    console.log(`RJP Sourcing Portal v3.2 listening on :${PORT}`);
    console.log(`  Apify Google actor:   ${process.env.APIFY_GOOGLE_ACTOR || process.env.APIFY_ACTOR || 'apify~rag-web-browser'}`);
    console.log(`  Apify LinkedIn actor: ${process.env.APIFY_LINKEDIN_ACTOR || 'harvestapi/linkedin-profile-scraper'}`);
    console.log(`  LLM client:           ${process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true' ? 'claude CLI subprocess (Max plan)' : (process.env.ANTHROPIC_API_KEY ? 'API key' : 'DISABLED')}`);
    console.log(`  Storage:              ${process.env.DATABASE_URL ? 'Postgres' : 'filesystem (./data, no persistence)'}`);
    console.log(`  Output dir:           ${process.env.OUTPUT_DIR || './outputs'}`);
    startWatchdog();
    store.startReconciliation();
  });
})().catch(e => {
  console.error('[boot] fatal:', e);
  process.exit(1);
});
