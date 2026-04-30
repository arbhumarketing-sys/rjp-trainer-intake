/**
 * RJP Sourcing Portal — Express server.
 *
 * Routes:
 *   POST /api/auth/login           { password } -> { token, user }
 *   GET  /api/briefs               (auth) list
 *   POST /api/briefs               (auth) create + kick pipeline
 *   GET  /api/briefs/:id           (auth) detail (status, log, counts)
 *   POST /api/briefs/:id/retry     (auth) re-run pipeline
 *   GET  /api/briefs/:id/output    (auth via header OR ?token=) download xlsx
 *   GET  /healthz                  liveness probe
 *
 * Statics:
 *   GET  /                         serves frontend/index.html if present
 */
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const auth = require('./auth');
const store = require('./store');
const { runPipeline, OUTPUT_DIR } = require('./pipeline');

const PORT = parseInt(process.env.PORT || '3000', 10);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);

const app = express();
app.use(express.json({ limit: '512kb' }));
app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);                    // curl, mobile, same-origin
    if (!ALLOWED_ORIGINS.length) return cb(null, true);    // permissive when not configured
    if (ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error('Origin not allowed: ' + origin));
  },
  credentials: false,
}));

/* Liveness */
app.get('/healthz', (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));

/* Auth */
app.post('/api/auth/login', (req, res) => {
  const result = auth.login(req.body && req.body.password);
  if (!result) return res.status(401).send('Wrong password');
  res.json(result);
});

/* Briefs */
app.get('/api/briefs', auth.requireAuth, (req, res) => {
  res.json({ briefs: store.list() });
});

app.post('/api/briefs', auth.requireAuth, (req, res) => {
  const body = req.body || {};
  if (!body.title || !Array.isArray(body.roles) || !body.roles.length) {
    return res.status(400).json({ error: 'title and at least one role required' });
  }
  const id = 'b_' + crypto.randomBytes(5).toString('hex') + '_' + Date.now().toString(36);
  const brief = {
    id,
    title: String(body.title).slice(0, 200),
    domain: String(body.domain || '').slice(0, 100),
    geo: String(body.geo || 'India').slice(0, 100),
    deadline: body.deadline || '',
    outputFormat: ['xlsx', 'pdf', 'both'].includes(body.outputFormat) ? body.outputFormat : 'xlsx',
    roles: (body.roles || []).slice(0, 25).map(r => ({
      title: String(r.title || '').slice(0, 200),
      skill: String(r.skill || '').slice(0, 200),
      bucket: String(r.bucket || '').slice(0, 100),
      count: Math.min(parseInt(r.count, 10) || 1, 200),
    })),
    steering: String(body.steering || '').slice(0, 4000),
    status: 'queued',
    log: [],
    counts: {},
    createdAt: new Date().toISOString(),
    submittedBy: req.user.team || 'rjp-infotek',
  };
  store.save(brief);
  // fire and forget
  setImmediate(() => runPipeline(id));
  res.json({ brief });
});

app.get('/api/briefs/:id', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  res.json({ brief: b });
});

app.post('/api/briefs/:id/retry', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  store.update(req.params.id, { status: 'queued', error: null, log: (b.log || []).concat([{ ts: new Date().toISOString(), msg: 'Retry requested', kind: 'info' }]) });
  setImmediate(() => runPipeline(req.params.id));
  res.json({ ok: true });
});

app.get('/api/briefs/:id/output', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).send('not found');
  if (!b.outputFile) return res.status(409).send('output not ready');
  const file = path.join(OUTPUT_DIR, b.outputFile);
  if (!fs.existsSync(file)) return res.status(404).send('file missing');
  const safeTitle = (b.title || 'sourcing-output').replace(/[^a-zA-Z0-9-_ ]/g, '_').slice(0, 80);
  // Filename must be Latin-1 only; force ASCII hyphen between title and id.
  res.setHeader('Content-Disposition', `attachment; filename="${safeTitle} - ${b.id}.xlsx"`);
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  fs.createReadStream(file).pipe(res);
});

/* Static frontend (optional — serve from same origin in single-service mode) */
const FRONTEND_DIR = path.resolve(__dirname, '..', 'frontend');
if (fs.existsSync(path.join(FRONTEND_DIR, 'index.html'))) {
  app.use(express.static(FRONTEND_DIR));
  app.get('/', (req, res) => res.sendFile(path.join(FRONTEND_DIR, 'index.html')));
}

app.use((err, req, res, next) => {
  console.error('[server error]', err);
  res.status(500).json({ error: err.message });
});

app.listen(PORT, () => {
  console.log(`RJP Sourcing Portal listening on :${PORT}`);
  console.log(`  Apify actor: ${process.env.APIFY_ACTOR || 'apify~rag-web-browser'}`);
  console.log(`  Data dir:    ${process.env.DATA_DIR || './data'}`);
  console.log(`  Output dir:  ${process.env.OUTPUT_DIR || './outputs'}`);
  console.log(`  Allowed origins: ${ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS.join(', ') : '(any)'}`);
});
