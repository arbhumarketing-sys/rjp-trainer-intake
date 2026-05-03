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
const { runPipeline, runWithFeedback, startWatchdog, reapOrphansOnBoot, OUTPUT_DIR, DEFAULT_BIGFIRM_EXCLUSIONS, clarifyInput } = require('./pipeline');
const { hasPerplexity } = require('./perplexity-client');
const { pool: apifyPool } = require('./apify-pool');

let Anthropic = null;
try { Anthropic = require('@anthropic-ai/sdk').default || require('@anthropic-ai/sdk'); } catch (_) {}

const _USE_CLI = process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true';
let ClaudeCliClient = null;
let getCliQueueStats = () => null;
if (_USE_CLI) {
  try {
    const cli = require('./anthropic-claude-cli');
    ClaudeCliClient = cli.ClaudeCliClient;
    if (cli.getCliQueueStats) getCliQueueStats = cli.getCliQueueStats;
  } catch (_) {}
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
  const cliQueue = getCliQueueStats();
  res.json({
    ok: true,
    ts: new Date().toISOString(),
    version: '3.20.0',
    uptimeSec: Math.round((Date.now() - _bootedAt) / 1000),
    storage: process.env.DATABASE_URL ? 'postgres' : 'filesystem',
    dirty,
    llm: process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true' ? 'claude-cli' : (process.env.ANTHROPIC_API_KEY ? 'api' : 'disabled'),
    cliQueue,
    perplexity: { configured: hasPerplexity() },
    apify: apifyPool.status(),  // v3.20.0 — per-account pool balances (cached, refresh in background)
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
    // v3.6: operator already engaged with the clarify-endpoint diagnostic
    // panel and chose to proceed. Pipeline skips its own keyword auto-cleaner.
    confirmedClean: !!body.confirmedClean,
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

// v3.2: hard-delete a brief and its persisted Excel.
// Refuses to delete a brief that's still running (status in non-terminal set)
// to avoid orphaning a pipeline mid-flight that would then write to a deleted
// record and recreate it. Operator must wait for completion or use the
// watchdog to fail it first.
app.delete('/api/briefs/:id', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const inFlight = ['queued', 'discovery', 'scoring', 'packaging', 'preview'].includes(b.status);
  if (inFlight && req.query.force !== '1') {
    return res.status(409).json({
      error: `brief is still ${b.status}; wait for completion or pass ?force=1`,
      status: b.status,
    });
  }
  const had = await store.deleteBrief(req.params.id);
  if (!had) return res.status(404).json({ error: 'not found (already deleted?)' });
  res.json({ ok: true, deleted: req.params.id });
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

/* v3.6: clarify-input endpoint. Wizard step 4 calls this on entry to get a
   pre-submit verdict (clear / needs_clarification / unsalvageable). Stateless
   — does NOT create a brief or run the pipeline. Cost: 1 Haiku call (~$0.001)
   only when input looks messy; clean briefs return immediately. */
app.post('/api/briefs/clarify', auth.requireAuth, async (req, res) => {
  const body = req.body || {};
  const briefDraft = {
    title:    String(body.title    || '').slice(0, 200),
    domain:   String(body.domain   || '').slice(0, 100),
    keywords: arrify(body.keywords).slice(0, 20),
    roles:    Array.isArray(body.roles) ? body.roles : [],
    must:     arrify(body.must).slice(0, 10),
    should:   arrify(body.should).slice(0, 10),
    mustNot:  arrify(body.mustNot).slice(0, 10),
    geo:      String(body.geo      || '').slice(0, 100),
  };
  try {
    const result = await clarifyInput(briefDraft, null);
    res.json(result);
  } catch (e) {
    console.error('[clarify] error:', e);
    // Don't block submission on clarify failures — fall through to "clear" so
    // the operator can still submit; the pipeline cleaner is the safety net.
    res.json({ status: 'clear', _clarifyError: e.message });
  }
});

/* v3.10.1: AskUserQuestion follow-up. Given a (possibly partial) brief draft,
   Haiku generates up to 15 multi-choice clarifying questions covering only the
   fields that are MISSING or AMBIGUOUS. Answers come back from the chat-style
   input UI as a clarifying-answers block appended to the draft's steering
   field — no schema changes required, the existing pipeline reads steering in
   every L1/L2/L3 prompt + the classifier + Sonnet rerank, so answers
   automatically inform the search. */
app.post('/api/briefs/ask-questions', auth.requireAuth, async (req, res) => {
  const draft = req.body || {};
  const client = makeLlmClient();
  if (!client) {
    return res.status(503).json({ error: 'Requires ANTHROPIC_VIA_CLAUDE_CLI=true or ANTHROPIC_API_KEY' });
  }
  const sys = `You are a sourcing assistant for an Indian B2B trainer-placement firm. Given a partial brief draft, generate up to 10 clarifying MULTI-CHOICE questions to refine the SEARCH. Focus on fields that are MISSING or AMBIGUOUS in the draft and that affect WHICH candidates surface. Skip fields the operator already specified clearly.

Output ONLY a JSON array of question objects. Each: {"id":"kebab-case-id","question":"human-readable question","choices":[{"id":"kebab-id","label":"human label"}],"multiSelect":boolean}. Each question MUST have between 2 and 5 choices.

Question coverage to consider (skip if already specified, ask only if missing/ambiguous):
1. Trainer (delivery) vs Consultant (advisory) vs Both
2. Years of experience required (5+, 8+, 12+, any)
3. Specific certifications relevant to the domain
4. Specific Indian cities or anywhere in India
5. Lab setup capability needed (yes/no/preferred)
6. Multi-corporate-client experience required
7. Number of trainers needed
8. Industry vertical preference (BFSI / IT services / pharma / manufacturing / any)
9. Languages preferred (English / Hindi / regional)
10. Big-firm exclusion: any extras beyond default Tech-M/Wipro/Cognizant/HCL/Infy/TCS/Accenture/Capgemini
11. Past customer companies to exclude (one-of-many existing clients)

DO NOT ASK about: delivery mode (online/in-person/hybrid), per-day rate / budget, lead time / availability / urgency, or content customization. These are NEGOTIATED post-shortlist with the trainer directly — they don't change which candidates the search should surface, so asking about them only adds friction. Skip them entirely even if the draft is silent.

Use multiSelect:true for questions where multiple answers make sense (cities, certifications, exclusions); false otherwise. Use natural human-friendly choice labels. NEVER ask about something the draft already specifies — the goal is to fill SEARCH-relevant GAPS, not re-ask known answers and not collect post-shortlist negotiation data.`;

  // Normalise the draft so parse-output and chat-input both work
  const normalized = {
    title:    String(draft.title || '').slice(0, 200),
    domain:   String(draft.domain || '').slice(0, 100),
    keywords: Array.isArray(draft.keywords) ? draft.keywords : (draft.keywords ? [draft.keywords] : []),
    must:     Array.isArray(draft.must) ? draft.must : (draft.must ? [draft.must] : []),
    should:   Array.isArray(draft.should) ? draft.should : (draft.should ? [draft.should] : []),
    mustNot:  Array.isArray(draft.mustNot) ? draft.mustNot : (draft.mustNot ? [draft.mustNot] : []),
    geo:      String(draft.geo || ''),
    searchMode: draft.searchMode === 'niche' ? 'niche' : 'std',
    deadline: String(draft.deadline || ''),
    customExclusions: Array.isArray(draft.customExclusions) ? draft.customExclusions : [],
    steering: String(draft.steering || '').slice(0, 1000),
    raw:      String(draft.raw || '').slice(0, 1500),
  };

  const user = `Brief draft:\n${JSON.stringify(normalized, null, 2)}\n\nGenerate up to 15 clarifying multi-choice questions covering ONLY missing/ambiguous fields. JSON array only — no prose.`;

  try {
    const resp = await client.messages.create({
      model: process.env.CLAUDE_MODEL_FAST || 'claude-haiku-4-5-20251001',
      max_tokens: 2400,
      system: sys,
      messages: [{ role: 'user', content: user }],
    });
    const text = (resp.content[0] && resp.content[0].text) || '';
    let questions = [];
    try {
      const m = text.match(/\[[\s\S]*\]/);
      if (m) questions = JSON.parse(m[0]);
    } catch (_) {}
    if (!Array.isArray(questions)) questions = [];
    questions = questions
      .filter(q => q && typeof q === 'object' && q.question && Array.isArray(q.choices) && q.choices.length >= 2 && q.choices.length <= 6)
      .slice(0, 15)
      .map((q, i) => ({
        id: String(q.id || ('q-' + i)).slice(0, 60),
        question: String(q.question).slice(0, 220),
        multiSelect: !!q.multiSelect,
        choices: q.choices.slice(0, 6).map((c, j) => ({
          id: String(c.id || ('c-' + j)).slice(0, 60),
          label: String(c.label || c.id || '').slice(0, 100),
        })).filter(c => c.label),
      }))
      .filter(q => q.choices.length >= 2);
    res.json({ questions });
  } catch (e) {
    console.error('[ask-questions] error:', e);
    res.status(500).json({ error: e.message });
  }
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

app.get('/api/briefs/:id/output', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).send('not found');
  if (!b.outputFile) return res.status(409).send('output not ready');

  const safeTitle = (b.title || 'sourcing-output').replace(/[^a-zA-Z0-9-_ ]/g, '_').slice(0, 80);
  const downloadName = `${safeTitle} - ${b.id}.xlsx`;

  // Try Postgres first — bytes there survive dyno restarts. Falls back to FS
  // for briefs created before the saveOutput hook was added (or if Postgres
  // is somehow unavailable).
  try {
    const stored = await store.getOutput(b.id);
    if (stored && stored.data) {
      res.setHeader('Content-Disposition', `attachment; filename="${downloadName}"`);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      return res.end(stored.data);
    }
  } catch (e) {
    console.warn('[server] getOutput from Postgres failed, falling back to FS:', e.message);
  }

  // Filesystem fallback. Defence in depth: verify resolved path stays under
  // OUTPUT_DIR so a future bug can't read /etc/passwd.
  const resolved = path.resolve(OUTPUT_DIR, b.outputFile);
  const outRoot = path.resolve(OUTPUT_DIR) + path.sep;
  if (!resolved.startsWith(outRoot) || !fs.existsSync(resolved)) {
    return res.status(404).send('file missing — Excel was not persisted to Postgres and the local copy is gone (likely a dyno restart). Re-run the brief to regenerate.');
  }
  res.setHeader('Content-Disposition', `attachment; filename="${downloadName}"`);
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

/* ---------- Client lifecycle status (v3.20.0) ----------
   PATCH /api/briefs/:id/client-status   { clientStatus, note? }
   Tracks the post-pipeline workflow: pending (default) → shared_with_client →
   candidate_booked / client_rejected / done_no_booking. Pipeline `status` stays
   focused on engine state (queued/discovery/...complete); clientStatus tracks
   the human workflow on top of a complete brief. */
const CLIENT_STATUS_VALUES = ['pending', 'shared_with_client', 'candidate_booked', 'client_rejected', 'done_no_booking'];
app.patch('/api/briefs/:id/client-status', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const body = req.body || {};
  const cs = String(body.clientStatus || '').toLowerCase();
  if (!CLIENT_STATUS_VALUES.includes(cs)) {
    return res.status(400).json({ error: `clientStatus must be one of: ${CLIENT_STATUS_VALUES.join(', ')}` });
  }
  const note   = String(body.note   || '').slice(0, 1000);
  const setBy  = String(body.setBy  || (req.user && req.user.team) || 'rjp-infotek').slice(0, 100);
  const history = (b.clientStatusHistory || []).concat([{
    clientStatus: cs, note, setBy, at: new Date().toISOString(),
  }]).slice(-50);  // cap history length
  store.update(req.params.id, {
    clientStatus: cs,
    clientStatusNote: note,
    clientStatusBy: setBy,
    clientStatusAt: new Date().toISOString(),
    clientStatusHistory: history,
  });
  res.json({ ok: true, clientStatus: cs, history });
});

/* ---------- Candidate scoring (v3.10.1) ----------
   GET    /api/briefs/:id/scores         — list scores for this brief
   POST   /api/briefs/:id/scores         — { candidateUrl, candidateName, score: 'selected'|'hold'|'rejected', note?, scoredBy? }
   DELETE /api/briefs/:id/scores         — body or query: { candidateUrl } removes a single score

   Each score becomes a learning signal for future reranks of similar briefs.
   Verdict taxonomy matches Saranya's client-side grading (v3.8.0) so scores
   compose with seed-client-gradings.json in a future rerank-priors ship. */
app.get('/api/briefs/:id/scores', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  try {
    const scores = await store.getCandidateScores(req.params.id);
    res.json({ scores });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/briefs/:id/scores', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const body = req.body || {};
  const candidateUrl  = String(body.candidateUrl  || '').slice(0, 500);
  const candidateName = String(body.candidateName || '').slice(0, 200);
  const score         = String(body.score         || '').toLowerCase();
  const note          = String(body.note          || '').slice(0, 1000);
  const scoredBy      = String(body.scoredBy      || (req.user && req.user.team) || 'rjp-infotek').slice(0, 100);
  if (!candidateUrl) return res.status(400).json({ error: 'candidateUrl required' });
  if (!['selected', 'hold', 'rejected'].includes(score)) {
    return res.status(400).json({ error: 'score must be one of: selected, hold, rejected' });
  }
  try {
    const saved = await store.saveCandidateScore(b.id, candidateUrl, candidateName, score, note, scoredBy);
    res.json({ score: saved });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Candidate outreach (v3.20.0) ----------
   GET    /api/briefs/:id/outreach
   POST   /api/briefs/:id/outreach   { candidateUrl, candidateName, status, note?, by? }
   DELETE /api/briefs/:id/outreach   body or query: { candidateUrl }
   Status: not_contacted | emailed | called | replied | scheduled | confirmed | no_response */
app.get('/api/briefs/:id/outreach', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  try {
    const outreach = await store.getCandidateOutreach(req.params.id);
    res.json({ outreach });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/briefs/:id/outreach', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const body = req.body || {};
  try {
    const saved = await store.saveCandidateOutreach(
      b.id,
      String(body.candidateUrl  || '').slice(0, 500),
      String(body.candidateName || '').slice(0, 200),
      String(body.status        || '').toLowerCase(),
      String(body.note          || '').slice(0, 1000),
      String(body.by            || (req.user && req.user.team) || 'rjp-infotek').slice(0, 100),
    );
    res.json({ outreach: saved });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/api/briefs/:id/outreach', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const url = String((req.body && req.body.candidateUrl) || req.query.candidateUrl || '');
  if (!url) return res.status(400).json({ error: 'candidateUrl required (in body or query)' });
  try {
    const removed = await store.removeCandidateOutreach(b.id, url);
    res.json({ ok: true, removed });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/briefs/:id/scores', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const url = String((req.body && req.body.candidateUrl) || req.query.candidateUrl || '');
  if (!url) return res.status(400).json({ error: 'candidateUrl required (in body or query)' });
  try {
    const removed = await store.removeCandidateScore(b.id, url);
    res.json({ ok: true, removed });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
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

  // Reap orphans BEFORE accepting new traffic. Any brief in a non-terminal
  // state at boot is necessarily an orphan from a previous (now-dead) dyno.
  reapOrphansOnBoot();

  app.listen(PORT, () => {
    console.log(`RJP Sourcing Portal v3.20.0 listening on :${PORT}`);
    console.log(`  Apify Google actor:   ${process.env.APIFY_GOOGLE_ACTOR || process.env.APIFY_ACTOR || 'apify~rag-web-browser'}`);
    console.log(`  Apify LinkedIn actor: ${process.env.APIFY_LINKEDIN_ACTOR || 'harvestapi/linkedin-profile-scraper'}`);
    console.log(`  LLM client:           ${process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true' ? 'claude CLI subprocess (Max plan)' : (process.env.ANTHROPIC_API_KEY ? 'API key' : 'DISABLED')}`);
    console.log(`  CLI semaphore:        max ${process.env.MAX_CONCURRENT_CLI || '2'} concurrent subprocesses`);
    console.log(`  Perplexity (L1.2):    ${hasPerplexity() ? `enabled (${process.env.PERPLEXITY_MODEL || 'sonar-pro'})` : 'DISABLED (set PERPLEXITY_API_KEY to enable)'}`);
    const _poolNames = apifyPool.status().accounts.map(a => a.name).join(', ') || '(none — set APIFY_TOKEN or APIFY_TOKEN_*)';
    console.log(`  Apify pool:           ${apifyPool.status().accountCount} account(s) [${_poolNames}]`);
    // Kick off initial balance refresh so healthz isn't blank after boot
    apifyPool.refresh(true).catch(e => console.warn('[boot] apify pool refresh failed:', e.message));
    console.log(`  Storage:              ${process.env.DATABASE_URL ? 'Postgres' : 'filesystem (./data, no persistence)'}`);
    console.log(`  Output dir:           ${process.env.OUTPUT_DIR || './outputs'}`);
    startWatchdog();
    store.startReconciliation();
  });
})().catch(e => {
  console.error('[boot] fatal:', e);
  process.exit(1);
});
