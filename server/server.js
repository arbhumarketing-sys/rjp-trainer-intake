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
    version: '3.39.0',
    uptimeSec: Math.round((Date.now() - _bootedAt) / 1000),
    storage: process.env.DATABASE_URL ? 'postgres' : 'filesystem',
    dirty,
    llm: (process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true' || process.env.ANTHROPIC_API_KEY) ? 'configured' : 'disabled',
    cliQueue,
    perplexity: { configured: hasPerplexity() },
    apify: apifyPool.status(),  // v3.27.0 — per-account pool balances (cached, refresh in background)
  });
});

// v3.27.0 — auth disabled (internal-only URL); /api/auth/login endpoint removed

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
    rawInput: String(body.rawInput || '').slice(0, 3000),
    // v3.33.0 — isTest flag separates internal/QA runs from real client briefs
    // so they don't pollute cross-brief priors or aggregate analytics.
    isTest: !!body.isTest,
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

app.get('/api/briefs/:id', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  // v3.36.0 — Cross-brief candidate memory: look up prior verdicts on the
  // candidate URLs visible in this brief's accepted/rejected samples. Cheap
  // single SQL query; gracefully degrades to empty map if Postgres is down.
  const sampleUrls = []
    .concat((b.acceptedSample || []).map(c => c && c.url).filter(Boolean))
    .concat((b.rejectedSample || []).map(c => c && c.url).filter(Boolean))
    .concat((b.previewSample  || []).map(c => c && c.url).filter(Boolean))
    .concat((b.previewRejected|| []).map(c => c && c.url).filter(Boolean));
  let priorVerdictMap = {};
  if (sampleUrls.length) {
    try { priorVerdictMap = await store.getPriorVerdictsByUrl(req.params.id, sampleUrls); }
    catch (e) { console.warn('[server] priorVerdictMap lookup failed:', e.message); }
  }
  res.json({ brief: { ...b, priorVerdictMap } });
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
    return res.status(503).json({ error: 'LLM service not configured. Contact admin.' });
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
/* v3.34.0 — Parse prompt rewritten with constraints + worked examples.
   The prior two-sentence prompt produced fragmented output: single-sentence
   inputs got shattered into 5+ word-keywords, and prose phrases ended up in
   must[] instead of crisp skill names. New prompt explicitly forbids those
   patterns and shows good-vs-bad examples drawn from real bad runs. */
const PARSE_SYSTEM_PROMPT = `You parse free-text trainer-sourcing briefs (typed by RJP staff in plain English, often pasted from WhatsApp / email / Word docs) into structured search input.

Output ONLY valid JSON with these keys: title (string), keywords (array), must (array), should (array), mustNot (array), clientCompany (string), customExclusions (array), searchMode ("std"|"niche"), deadline (string), notes (string).

CRITICAL RULES for \`keywords\` (drives Google searches downstream):
- 1 to 4 entries MAX. Quality over quantity. Each keyword runs a separate web search — extras burn quota without adding signal.
- Each keyword must be a COMPLETE searchable phrase (typically 2-5 words) capturing one coherent concept. NOT a fragment.
- NEVER produce single generic nouns alone — like "trainer", "consultant", "expert", "tool", "implementation", "experts", "professional", "developer", "engineer", "specialist", "instructor", "India", "online", "freelance". They match anyone. The pipeline already adds "trainer India" context to every search — don't duplicate.
- COMBINE fragmented concepts into one keyword. "implementation experts on Base.com" is ONE keyword: "Base.com implementation expert" — not five split words.
- DO NOT pad with role variants. "Splunk Trainer" + "Splunk Consultant" + "Splunk Expert" + "Splunk SME" all hit the same pool — pick one. The downstream rerank handles role variants automatically.

CRITICAL RULES for \`must\` (the hard filter — every accepted candidate must demonstrate this):
- 1 to 3 entries. Each is a SINGLE crisp technical skill or product name (e.g., "Splunk", "WooCommerce", "Base.com", "AWS Aurora", "NetBrain").
- NOT prose, NOT phrases. "ecommerce prior experience" → "ecommerce". "12+ years experience" → goes to should[], not must[].
- If the brief mentions a specific product/platform, that's the must.

\`should\` (nice-to-have): same crisp format. 0-3 entries. Soft signals like seniority years, side-skills, certifications.

\`mustNot\`: terms that disqualify. Same crisp format.

\`searchMode\`:
- "niche" if: technology is rare/specialized (NetBrain, Snowflake DBA, very specific tools), OR brief explicitly says "freelance only", OR generic discovery is unlikely to surface enough specialists.
- "std" otherwise.

EXAMPLES:

Input: "implementation experts on base.com tool — ecommerce prior experience is needed also"
Output: { "title": "Base.com Implementation Expert with Ecommerce Background", "keywords": ["Base.com implementation expert"], "must": ["Base.com", "ecommerce"], "should": [], "mustNot": [], "searchMode": "niche", "notes": "Single coherent concept; Base.com is niche so niche mode." }

Input: "5 Splunk Cloud Observability trainers, India, exclude TCS, niche, freelance only"
Output: { "title": "Splunk Cloud Observability Trainers (Freelance, ex-TCS)", "keywords": ["Splunk Cloud Observability trainer"], "must": ["Splunk"], "should": ["Splunk Cloud", "Observability"], "mustNot": ["TCS"], "customExclusions": ["TCS"], "searchMode": "niche", "notes": "Freelance + niche tech → niche mode. 5 candidates wanted." }

Input: "WooCommerce trainer with skils in in ecommerce pluggins experience"
Output: { "title": "WooCommerce Trainer", "keywords": ["WooCommerce trainer"], "must": ["WooCommerce"], "should": ["ecommerce plugins"], "mustNot": [], "searchMode": "std", "notes": "" }

Input: "Need an SAP S/4HANA FICO consultant in Bengaluru with 12+ years and lab setup"
Output: { "title": "SAP S/4HANA FICO Consultant — Bengaluru, 12+yr, lab-equipped", "keywords": ["SAP S/4HANA FICO consultant"], "must": ["SAP S/4HANA", "FICO"], "should": ["12+ years experience", "lab access"], "mustNot": [], "searchMode": "std", "notes": "Bengaluru preferred but other India cities likely acceptable." }

Input: "splunk trainer / consultant / expert / SME / observability architect"
Output: { "title": "Splunk Observability Trainer / SME", "keywords": ["Splunk Observability trainer"], "must": ["Splunk"], "should": ["Observability"], "mustNot": [], "searchMode": "std", "notes": "Multiple role suffixes are noise — the rerank handles variants." }`;

app.post('/api/briefs/parse', auth.requireAuth, async (req, res) => {
  const text = String((req.body && req.body.text) || '').slice(0, 2000).trim();
  if (!text) return res.status(400).json({ error: 'text required' });
  const client = makeLlmClient();
  if (!client) {
    return res.status(503).json({ error: 'Free-text parsing service not configured. Contact admin.' });
  }
  try {
    const resp = await client.messages.create({
      model: process.env.CLAUDE_MODEL_FAST || 'claude-haiku-4-5-20251001',
      max_tokens: 800,
      system: PARSE_SYSTEM_PROMPT,
      messages: [{ role: 'user', content: `Parse this brief into JSON:\n\n"""${text}"""\n\nReturn ONLY the JSON object.` }],
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

/* v3.34.0 — Parse-critique pass. Operator clicks "Continue to follow-ups" on
   Step 2. Backend compares the raw input to the (possibly hand-edited) parsed
   fields and returns:
     - confident_rewrites: high-confidence corrections (auto-apply with consent)
     - issues: softer warnings to surface inline
   Replaces the v3.32.0 static heuristics, which were patching around parser
   bugs with hardcoded rules. With this pass + the rewritten parse prompt,
   the operator gets contextual, polite, brief-specific feedback. */
const CRITIQUE_SYSTEM_PROMPT = `You are a search-quality reviewer for a trainer-sourcing pipeline. The operator wrote a free-text brief; our parser extracted structured fields. Sometimes operators edit the fields after parsing.

Your job: given the raw input AND the current parsed fields, identify SPECIFIC problems and propose CORRECTIONS. Be polite, concise, and concrete. Reference the operator's exact words where possible.

Output ONLY valid JSON with this shape:
{
  "issues": [
    { "type": "fragmentation"|"redundancy"|"generic"|"missing_must"|"prose_must"|"off_target"|"thin_market", "severity": "block"|"warn"|"info", "title": "short headline", "message": "1-2 sentence polite explanation", "field": "keywords"|"must"|"should"|"mustNot"|"" }
  ],
  "confident_rewrites": [
    { "field": "keywords"|"must"|"should"|"mustNot", "oldValue": [...], "newValue": [...], "reason": "1-sentence rationale grounded in the operator's raw text" }
  ]
}

RULES for issues:
- Use "block" severity ONLY for: missing must-have entirely.
- Use "warn" for: 5+ keywords, redundant variants, prose-shaped must-haves, generic single-noun keywords, off-target keywords (don't match raw input).
- Use "info" for: market-thinness signals worth noting, edge cases.

RULES for confident_rewrites:
- Only include corrections you are >=85% confident in.
- newValue must improve on oldValue based on the raw input.
- For fragmented keywords: combine into ONE coherent phrase.
- For redundant role variants: keep the strongest one.
- For prose must-haves: extract the crisp skill term.

EXAMPLES:

Raw: "implementation experts on base.com tool — ecommerce prior experience is needed also"
Parsed keywords: ["base.com", "implementation", "ecommerce", "tool", "experts"]
Parsed must: ["ecommerce prior experience", "base.com tool implementation expertise"]
Output: {
  "issues": [],
  "confident_rewrites": [
    { "field": "keywords", "oldValue": ["base.com", "implementation", "ecommerce", "tool", "experts"], "newValue": ["Base.com implementation expert"], "reason": "These five keywords are fragments of one concept from your description ('implementation experts on Base.com tool'). Combining them sharpens the search." },
    { "field": "must", "oldValue": ["ecommerce prior experience", "base.com tool implementation expertise"], "newValue": ["Base.com", "ecommerce"], "reason": "Must-have works best as crisp skill terms. We extracted Base.com and ecommerce from your description." }
  ]
}

Raw: "5 Splunk Observability trainers India freelance"
Parsed keywords: ["Splunk Observability Trainer", "Splunk Observability Consultant", "Splunk Observability SME", "Splunk Observability Expert", "Splunk Observability Architect", "Splunk Observability Solution Architect"]
Parsed must: []
Output: {
  "issues": [
    { "type": "missing_must", "severity": "block", "title": "Add a must-have skill", "message": "Without 'Splunk' as a must-have, candidates without genuine Splunk depth will pass the filter. Recommended: add 'Splunk' to must-have.", "field": "must" }
  ],
  "confident_rewrites": [
    { "field": "keywords", "oldValue": ["Splunk Observability Trainer","Splunk Observability Consultant","Splunk Observability SME","Splunk Observability Expert","Splunk Observability Architect","Splunk Observability Solution Architect"], "newValue": ["Splunk Observability trainer"], "reason": "All six keywords target the same pool with different role suffixes. The ranking step already handles role variants — one sharp keyword does the job." }
  ]
}

Raw: "woocomerce trainer with skils in in ecommerce pluggins experience"
Parsed keywords: ["WooCommerce", "ecommerce", "plugins", "ecommerce plugins"]
Parsed must: ["WooCommerce experience", "ecommerce plugins knowledge"]
Output: {
  "issues": [],
  "confident_rewrites": [
    { "field": "keywords", "oldValue": ["WooCommerce","ecommerce","plugins","ecommerce plugins"], "newValue": ["WooCommerce trainer"], "reason": "Your description was 'WooCommerce trainer with ecommerce plugins skill' — that's one coherent search target. The other three keywords overlap and add noise." },
    { "field": "must", "oldValue": ["WooCommerce experience", "ecommerce plugins knowledge"], "newValue": ["WooCommerce"], "reason": "Must-have works best as a single crisp skill. WooCommerce is the core requirement; ecommerce plugins is a should-have." },
    { "field": "should", "oldValue": [], "newValue": ["ecommerce plugins"], "reason": "Moved from must to should — it's a nice-to-have, not a hard filter." }
  ]
}

Now review:`;

app.post('/api/briefs/critique', auth.requireAuth, async (req, res) => {
  const rawInput = String((req.body && req.body.rawInput) || '').slice(0, 3000).trim();
  const parsed = (req.body && typeof req.body.parsed === 'object') ? req.body.parsed : {};
  if (!rawInput && !parsed) return res.status(400).json({ error: 'rawInput or parsed required' });
  const client = makeLlmClient();
  if (!client) {
    return res.status(503).json({ error: 'Critique service not configured. Continuing without critique.' });
  }
  try {
    const userPrompt = `Raw input from operator:\n"""${rawInput}"""\n\nCurrent parsed fields:\n${JSON.stringify({
      keywords: parsed.keywords || [],
      must:     parsed.must     || [],
      should:   parsed.should   || [],
      mustNot:  parsed.mustNot  || [],
      searchMode: parsed.searchMode || 'std',
    }, null, 2)}\n\nReturn ONLY the JSON object with issues and confident_rewrites.`;
    const resp = await client.messages.create({
      model: process.env.CLAUDE_MODEL_FAST || 'claude-haiku-4-5-20251001',
      max_tokens: 1200,
      system: CRITIQUE_SYSTEM_PROMPT,
      messages: [{ role: 'user', content: userPrompt }],
    });
    const out = (resp.content[0] && resp.content[0].text) || '';
    let critique = { issues: [], confident_rewrites: [] };
    try {
      const m = out.match(/\{[\s\S]*\}/);
      if (m) critique = JSON.parse(m[0]);
    } catch (_) {}
    if (!Array.isArray(critique.issues)) critique.issues = [];
    if (!Array.isArray(critique.confident_rewrites)) critique.confident_rewrites = [];
    res.json({ critique });
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

// v3.27.0 — Admin endpoints removed (Admin tab gone; 0 persistent exclusions
// + 1 feature-request total ever, no UI consumed them). store.js retains the
// Postgres-backed helpers so historical rows stay readable if ever needed,
// just no HTTP surface anymore.

/* ---------- Client lifecycle status (v3.27.0) ----------
   PATCH /api/briefs/:id/client-status   { clientStatus, note? }
   Tracks the post-pipeline workflow: pending (default) → shared_with_client →
   candidate_booked / client_rejected / done_no_booking. Pipeline `status` stays
   focused on engine state (queued/discovery/...complete); clientStatus tracks
   the human workflow on top of a complete brief. */
/* v3.38.0 — Post-run quality report (Part 1 #1.a). When operator opens a
   completed brief they get an honest diagnostic: was the run weak because of
   their input, the market, or the engine? Combines rule-based facts (counts,
   ratios, keyword heuristics) with an LLM-synthesised prose summary plus 1-3
   concrete recommendations for the operator's next brief. Only useful for
   completed runs; returns 409 for in-flight ones. */
app.post('/api/briefs/:id/quality-report', auth.requireAuth, async (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  if (b.status !== 'complete' && b.status !== 'failed') {
    return res.status(409).json({ error: 'Quality report only available for completed runs' });
  }

  // ---- Rule-based facts ----
  const keywords = b.keywords || [];
  const must     = b.must || [];
  const counts   = b.counts || {};
  const accepted = counts.accepted || 0;
  const rejected = counts.rejected || 0;
  const discovered = counts.discovered || 0;
  const acceptRate = (accepted + rejected) > 0 ? Math.round((accepted / (accepted + rejected)) * 100) : 0;

  const GENERIC = new Set(['trainer','consultant','expert','sme','india','indian','specialist','professional','developer','engineer','instructor','teacher','coach','architect','tool','experts']);
  const genericKeywords = keywords.filter(k => GENERIC.has(String(k || '').trim().toLowerCase()));
  const proseMust = must.filter(m => String(m || '').split(/\s+/).length > 3);

  // Peer comparison: similar mode, non-test, completed
  const allBriefs = store.list();
  const peers = allBriefs.filter(x => x.id !== b.id && x.status === 'complete' && !x.isTest && x.mode === b.mode && (x.counts || {}).accepted != null);
  const peerAcceptRates = peers.map(x => {
    const a = x.counts.accepted || 0, r = x.counts.rejected || 0;
    return (a + r) > 0 ? (a / (a + r)) * 100 : 0;
  }).filter(v => v > 0).sort((a, b) => a - b);
  const peerMedianAcceptRate = peerAcceptRates.length ? Math.round(peerAcceptRates[Math.floor(peerAcceptRates.length / 2)]) : null;

  const peerDiscovery = peers.map(x => x.counts.discovered || 0).filter(v => v > 0).sort((a, b) => a - b);
  const peerMedianDiscovery = peerDiscovery.length ? peerDiscovery[Math.floor(peerDiscovery.length / 2)] : null;

  const facts = {
    inputs: {
      keywordCount: keywords.length,
      genericKeywords,
      mustCount: must.length,
      proseMust,
      hasNoMustHave: must.length === 0,
      searchMode: b.mode,
    },
    outcome: {
      discovered, accepted, rejected, acceptRate,
      lowYield: !!b.lowYield,
      lowYieldReason: b.lowYieldReason || null,
      preflight: b.preflight || null,
    },
    peerComparison: {
      peerCount: peers.length,
      peerMedianAcceptRate,
      peerMedianDiscovery,
      betterThanPeers: peerMedianAcceptRate != null && acceptRate > peerMedianAcceptRate + 5,
      worseThanPeers:  peerMedianAcceptRate != null && acceptRate < peerMedianAcceptRate - 5,
    },
    feedbackIterations: ((b.iterationSummary || []).length) - 1,  // excluding the original run
  };

  // ---- LLM-synthesised summary + recommendations ----
  // Wrapped in try so the rule-based facts always return even if LLM is down.
  let summary = '';
  let recommendations = [];
  try {
    const client = makeLlmClient();
    if (client) {
      const sys = `You are a sourcing-quality coach. Given a brief and its run results, write a SHORT (max 4 sentences), HONEST, NON-JUDGEMENTAL diagnostic of why the run performed as it did. Then list 1-3 concrete recommendations for the operator's next brief.

Output ONLY valid JSON: { "summary": "...", "recommendations": ["...", "..."] }

Be specific about CAUSES:
- "Input quality" = operator typed weak keywords / no must-have / generic terms
- "Market thinness" = the technology genuinely has few practitioners
- "Engine issue" = something in the pipeline retried or failed
- "Worked well" = run was strong; reinforce what to keep doing

Recommendations should be ACTIONABLE and SPECIFIC. Reference the operator's actual keywords/must-have where relevant.`;
      const user = `Brief title: ${b.title || ''}
Original raw input: """${(b.rawInput || '').slice(0, 600)}"""
Keywords: ${JSON.stringify(keywords)}
Must-have: ${JSON.stringify(must)}
Search mode: ${b.mode}
Discovered ${discovered} candidates, accepted ${accepted}, rejected ${rejected} (${acceptRate}% accept rate).
${peerMedianAcceptRate != null ? `Peers (similar mode, ${peers.length} runs) median accept rate: ${peerMedianAcceptRate}%.` : 'Not enough peer data yet.'}
${b.lowYield ? `Run flagged as low-yield. Reason: ${b.lowYieldReason || ''}` : ''}
${b.preflight ? `Pre-flight defensibility: ${b.preflight.yesCount}/${b.preflight.sampled} candidates passed the "would you send to client" gate.` : ''}
${facts.feedbackIterations > 0 ? `Operator re-ran with feedback ${facts.feedbackIterations} time${facts.feedbackIterations > 1 ? 's' : ''}.` : ''}

Write the JSON now.`;

      const resp = await client.messages.create({
        model: process.env.CLAUDE_MODEL_FAST || 'claude-haiku-4-5-20251001',
        max_tokens: 600,
        system: sys,
        messages: [{ role: 'user', content: user }],
      });
      const out = (resp.content[0] && resp.content[0].text) || '';
      try {
        const m = out.match(/\{[\s\S]*\}/);
        if (m) {
          const j = JSON.parse(m[0]);
          summary = String(j.summary || '').slice(0, 600);
          if (Array.isArray(j.recommendations)) recommendations = j.recommendations.slice(0, 4).map(r => String(r).slice(0, 250));
        }
      } catch (_) {}
    }
  } catch (e) {
    console.warn('[quality-report] LLM summary failed:', e.message);
  }

  res.json({ facts, summary, recommendations });
});

// v3.33.0 — PATCH the isTest flag on an existing brief. Used to backfill old
// internal QA briefs so they stop polluting priors and analytics.
app.patch('/api/briefs/:id/test-flag', auth.requireAuth, (req, res) => {
  const b = store.get(req.params.id);
  if (!b) return res.status(404).json({ error: 'not found' });
  const isTest = !!(req.body && req.body.isTest);
  store.update(req.params.id, { isTest });
  res.json({ ok: true, isTest });
});

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

/* ---------- Candidate outreach (v3.27.0) ----------
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
    console.log(`RJP Sourcing Portal v3.27.0 listening on :${PORT}`);
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
