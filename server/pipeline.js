/**
 * RJP Sourcing Pipeline — v3.1
 *
 * Rebuilt against the team's manual SOP + operator feedback.
 * Replaces the v3 single-source Apify pipeline. Key structural changes:
 *   - Multi-keyword input (operator feedback). Each keyword runs its own L1.
 *   - Boolean operators on Google queries (operator feedback).
 *   - Three-pronged exclusion: client company / principal / custom (operator feedback).
 *   - Speaker-aware classifier — speakers ≠ trainers (per SOP).
 *   - Default big-firm exclusion: Tech M / Cognizant / Wipro / HCL / Infy / TCS / Accenture / Capgemini.
 *   - L1→L4 source cascade: Claude knowledge → Claude+web → Apify Google → harvestapi-linkedin.
 *   - L2 adjacent-tech expansion via Claude if L1 is thin.
 *   - L3 institutes lookup via Claude if Niche mode and still thin.
 *   - L4 Udemy + YouTube fallback (Niche + still thin).
 *   - Geo-strict via harvestapi-linkedin location field (Hari Babu Matta US slip).
 *   - Reason-traceable: every accepted profile gets decision_url + snippet + 1-line why.
 *                       every rejected profile gets a 1-line rejection_reason.
 *   - Pre-flight preview: 1 query → 5 sample profiles → user adjusts → full run.
 *   - Per-stage TAT logging. Target 8 min total for Std, 12 min for Niche.
 *   - Quality cap: 5–15 strong matches per brief, not 50.
 *
 * Dependencies:
 *   APIFY_TOKEN          — required for Google + LinkedIn actors
 *   APIFY_GOOGLE_ACTOR   — default 'apify~rag-web-browser'
 *   APIFY_LINKEDIN_ACTOR — default 'harvestapi/linkedin-profile-scraper'
 *   ANTHROPIC_API_KEY    — optional. If missing, Claude L1.1 / L1.2 / L2 / L3 are skipped
 *                          and the pipeline falls back to Google-only with a warning.
 */
const fs = require('fs');
const path = require('path');
const { ApifyClient } = require('apify-client');
const ExcelJS = require('exceljs');
const store = require('./store');

let Anthropic = null;
try { Anthropic = require('@anthropic-ai/sdk').default || require('@anthropic-ai/sdk'); } catch (_) { /* optional */ }

const _USE_CLI = process.env.ANTHROPIC_VIA_CLAUDE_CLI === 'true';
let ClaudeCliClient = null;
if (_USE_CLI) {
  try { ClaudeCliClient = require('./anthropic-claude-cli').ClaudeCliClient; } catch (_) { /* optional */ }
}
function hasLlmClient() {
  if (_USE_CLI) return !!ClaudeCliClient;
  return !!(Anthropic && process.env.ANTHROPIC_API_KEY);
}

const OUTPUT_DIR = process.env.OUTPUT_DIR || path.join(__dirname, 'outputs');
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

const APIFY_GOOGLE_ACTOR   = process.env.APIFY_GOOGLE_ACTOR   || process.env.APIFY_ACTOR || 'apify~rag-web-browser';
const APIFY_LINKEDIN_ACTOR = process.env.APIFY_LINKEDIN_ACTOR || 'harvestapi/linkedin-profile-scraper';

const MAX_RESULTS_PER_QUERY = parseInt(process.env.MAX_RESULTS_PER_QUERY || '15', 10);
const MAX_QUERIES_STD       = parseInt(process.env.MAX_QUERIES_STD       || '12', 10);
const MAX_QUERIES_NICHE     = parseInt(process.env.MAX_QUERIES_NICHE     || '20', 10);
const QUALITY_CAP_DEFAULT   = parseInt(process.env.QUALITY_CAP           || '15', 10);
const PREVIEW_SAMPLE_SIZE   = 5;

const CLAUDE_FAST   = process.env.CLAUDE_MODEL_FAST   || 'claude-haiku-4-5-20251001';
const CLAUDE_SMART  = process.env.CLAUDE_MODEL_SMART  || 'claude-sonnet-4-5';

/* ---------- Defaults: exclusion list (big Indian system integrators) ---------- */
const DEFAULT_BIGFIRM_EXCLUSIONS = [
  'tech mahindra', 'cognizant', 'wipro', 'hcl technologies', 'hcltech', 'hcl',
  'infosys', 'tata consultancy', 'tcs', 'accenture', 'capgemini',
  'mindtree', 'ltimindtree', 'mphasis', 'ibm india', 'oracle india',
];

const INDIAN_CITIES = [
  'bengaluru', 'bangalore', 'mumbai', 'pune', 'hyderabad', 'chennai',
  'delhi', 'new delhi', 'gurgaon', 'gurugram', 'noida', 'kolkata',
  'ahmedabad', 'kochi', 'cochin', 'thiruvananthapuram', 'trivandrum',
  'jaipur', 'indore', 'coimbatore', 'mysore', 'mysuru', 'visakhapatnam',
  'vizag', 'nagpur', 'lucknow', 'bhubaneswar', 'chandigarh', 'goa',
  'mohali', 'panchkula',
];

/* ---------- Logging ---------- */
function logAndSave(briefId, msg, kind = 'info', meta = null) {
  const tag = '[pipeline:' + briefId + ']';
  console.log(tag, msg, meta || '');
  store.appendLog(briefId, msg, kind, meta);
}

function elapsedSec(t0) { return ((Date.now() - t0) / 1000).toFixed(1); }

/* ---------- Retry helper ---------- */
// Wrap external calls so a transient blip (network jitter, Apify worker stall,
// Claude CLI rate-limit window) doesn't downgrade a brief from "8 trainers"
// to "0 trainers, looks broken". The tier's outer try/catch still returns []
// on terminal failure, so this just buys more chances of a non-empty result
// before giving up — graceful degradation preserved.
//
// Status mapping for the Claude CLI client (anthropic-claude-cli.js):
//   401 = auth/login required        → DON'T retry (will fail forever)
//   400 = bad request                 → DON'T retry
//   500 = generic CLI/transport error → retry (often a rate-limit window edge)
//   502 = CLI returned non-JSON       → retry (likely flake)
//   504 = subprocess timeout          → retry (network or Claude API slow)
// Apify errors from apify-client are HTTP errors; treat 4xx (except 408/429)
// as terminal and everything else as transient.
async function withRetry(fn, opts = {}) {
  const maxAttempts = opts.maxAttempts || 3;
  const baseDelayMs = opts.baseDelayMs || 2000;
  const onRetry = opts.onRetry || (() => {});
  const isRetryable = opts.isRetryable || ((err) => {
    const s = err && err.status;
    if (s == null) return true;             // network/unknown — assume transient
    if (s === 408 || s === 429) return true; // request timeout / rate limit
    if (s >= 400 && s < 500) return false;   // other 4xx — terminal
    return true;                              // 5xx and unknown — transient
  });
  let lastErr;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn(attempt);
    } catch (err) {
      lastErr = err;
      if (attempt === maxAttempts || !isRetryable(err)) throw err;
      const expDelay = baseDelayMs * Math.pow(2, attempt - 1);
      const jitter = Math.random() * baseDelayMs * 0.5;
      const delay = expDelay + jitter;
      try { onRetry(attempt, delay, err); } catch (_) { /* logger must never throw */ }
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

/* ---------- Brief normalization ---------- */
function normalizeBrief(brief) {
  // Backward-compat with v3 briefs that only have domain + roles.
  const keywords = (brief.keywords && brief.keywords.length)
    ? brief.keywords
    : buildKeywordsFromRoles(brief);
  const must = brief.must || [];
  const should = brief.should || [];
  const mustNot = brief.mustNot || [];
  const clientCompany = (brief.clientCompany || '').trim();
  const clientPrincipal = (brief.clientPrincipal || '').trim();
  const customExclusions = (brief.customExclusions || []).map(s => s.trim()).filter(Boolean);
  const searchMode = brief.searchMode === 'niche' ? 'niche' : 'std';
  const advanced = Object.assign({
    queryDepth: searchMode === 'niche' ? MAX_QUERIES_NICHE : MAX_QUERIES_STD,
    sources: { linkedin: true, urbanpro: true, youtube: true, udemy: true, blogs: true },
    weights: { signal: 40, bucket: 30, verify: 15, book: 15 },
    qualityCap: QUALITY_CAP_DEFAULT,
  }, brief.advanced || {});
  const exclusions = dedupeLower([
    ...DEFAULT_BIGFIRM_EXCLUSIONS,
    ...(clientCompany ? [clientCompany] : []),
    ...(clientPrincipal ? [clientPrincipal] : []),
    ...customExclusions,
  ]);
  // Steering — operator's free-text direction + accumulated Form-1 feedback. Capped
  // so multi-revision feedback histories don't blow the LLM prompt budget. Latest
  // feedback is always at the end (runWithFeedback appends), so tail-slice.
  const steering = (brief.steering || '').slice(-2500);
  return { keywords, must, should, mustNot, clientCompany, clientPrincipal, customExclusions, searchMode, advanced, exclusions, steering };
}

// Convert steering text into a prompt-safe directive block. Returns '' when empty.
function steeringHint(steering) {
  if (!steering || !steering.trim()) return '';
  return `\n\nOPERATOR STEERING (apply when ranking / generating candidates):\n${steering.trim()}`;
}

function buildKeywordsFromRoles(brief) {
  const out = [];
  const domain = (brief.domain || '').trim();
  for (const r of (brief.roles || [])) {
    const seed = [r.skill, r.title].filter(Boolean).join(' ').trim() || domain;
    if (seed) out.push(seed);
  }
  return out.length ? out : (domain ? [domain] : []);
}

function dedupeLower(arr) {
  const seen = new Set();
  const out = [];
  for (const s of arr) {
    const k = (s || '').toLowerCase().trim();
    if (!k || seen.has(k)) continue;
    seen.add(k);
    out.push(s);
  }
  return out;
}

/* ---------- Query construction (no exact-phrase wrapping — the Siebel bug) ---------- */
function buildGoogleQueries(brief, bp) {
  // Per the manual SOP: "LinkedIn + independent instructor + <tech>"
  // Plus boolean ops from must/should/mustNot.
  const queries = [];
  const sites = bp.advanced.sources;
  const trainerVariants = ['independent instructor', 'freelance trainer', 'corporate trainer', 'instructor', 'trainer'];

  for (const kw of bp.keywords) {
    const kwClean = sanitizeQueryTerm(kw);
    for (const variant of trainerVariants) {
      // Base: LinkedIn + variant + keyword + India
      // Boolean: must (AND), should (OR), mustNot (-)
      const must = bp.must.map(m => sanitizeQueryTerm(m)).filter(Boolean).map(m => `"${m}"`).join(' ');
      const should = bp.should.map(s => sanitizeQueryTerm(s)).filter(Boolean).map(s => `"${s}"`).join(' OR ');
      const mustNot = bp.mustNot.map(m => sanitizeQueryTerm(m)).filter(Boolean).map(m => `-"${m}"`).join(' ');
      const exclusions = bp.exclusions.slice(0, 5).map(e => `-"${e}"`).join(' ');

      if (sites.linkedin) {
        queries.push({
          query: `${variant} ${kwClean} India site:linkedin.com/in ${must} ${should ? '(' + should + ')' : ''} ${mustNot} ${exclusions}`.replace(/\s+/g, ' ').trim(),
          keyword: kw, variant, source: 'linkedin', tier: 'L1.3',
        });
      }
    }
    // UrbanPro / Sulekha
    if (sites.urbanpro) {
      queries.push({
        query: `${kwClean} trainer India site:urbanpro.com OR site:sulekha.com`,
        keyword: kw, variant: 'marketplace', source: 'urbanpro', tier: 'L1.3',
      });
    }
    // Blog/website
    if (sites.blogs) {
      queries.push({
        query: `${kwClean} freelance trainer India -site:linkedin.com`,
        keyword: kw, variant: 'blog', source: 'blogs', tier: 'L1.3',
      });
    }
  }
  return queries.slice(0, bp.advanced.queryDepth);
}

function sanitizeQueryTerm(s) {
  // Strip slashes (the Siebel bug — "Consultant / Trainer" broke Google).
  // Strip excess whitespace and quotes.
  return String(s || '')
    .replace(/[\/\\]/g, ' ')
    .replace(/["']/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/* ---------- Apify: Google search ---------- */
async function runGoogleQuery(client, q, briefId) {
  if (process.env.MOCK_APIFY === '1') return mockGoogle(q);
  try {
    const input = {
      query: q.query,
      maxResults: MAX_RESULTS_PER_QUERY,
      outputFormats: ['markdown'],
      scrapingTool: 'raw-http',
      requestTimeoutSecs: 25,
    };
    const run = await withRetry(
      () => client.actor(APIFY_GOOGLE_ACTOR).call(input, { waitSecs: 150 }),
      {
        maxAttempts: 3,
        baseDelayMs: 2000,
        onRetry: (attempt, delay, err) => {
          if (briefId) logAndSave(briefId, `[L1.3 retry] Apify Google attempt ${attempt} failed (${(err.message || 'unknown').slice(0, 80)}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );
    if (!run || !run.defaultDatasetId) return [];
    const ds = await withRetry(
      () => client.dataset(run.defaultDatasetId).listItems(),
      { maxAttempts: 3, baseDelayMs: 1500 }
    );
    return (ds.items || []).map(it => ({ ...it, _q: q }));
  } catch (e) {
    if (briefId) logAndSave(briefId, `[L1.3] Apify Google gave up after retries: ${(e.message || 'unknown').slice(0, 100)}`, 'warn');
    console.warn('[google query failed]', q.query.slice(0, 60), '→', e.message);
    return [];
  }
}

function mockGoogle(q) {
  // Synthetic — exercises the speaker-vs-trainer + big-firm + geo logic.
  // Override via env: MOCK_FIXTURES_FILE=/path/to/fixtures.json (array of profile objects).
  let fixtures;
  if (process.env.MOCK_FIXTURES_FILE && fs.existsSync(process.env.MOCK_FIXTURES_FILE)) {
    try { fixtures = JSON.parse(fs.readFileSync(process.env.MOCK_FIXTURES_FILE, 'utf8')); }
    catch (_) { fixtures = null; }
  }
  if (!fixtures) fixtures = [
    { name: 'Prabu T', headline: 'Independent Instructor — Aurora PostgreSQL & DBA training', linkedin: 'in/prabu-t-b5370761', loc: 'Chennai, India', firm: 'Independent', delivers_training: true },
    { name: 'Prasanna (worldofprasanna)', headline: 'Corporate Trainer — Cloud, PostgreSQL, DevOps', linkedin: 'in/worldofprasanna', loc: 'Bengaluru, India', firm: 'Independent', delivers_training: true },
    { name: 'Atulpriya Sharma', headline: 'CNCF Ambassador, KubeCon Speaker — InfraCloud', linkedin: 'in/atulpriya', loc: 'Pune, India', firm: 'InfraCloud', delivers_training: false, speaker_only: true },
    { name: 'Hari Babu Matta', headline: 'Siebel to Salesforce Migration Expert', linkedin: 'in/hari-babu-matta', loc: 'Plano, Texas, United States', firm: 'Independent', delivers_training: false },
    { name: 'Generic Wipro Engineer', headline: 'Senior Salesforce Developer at Wipro', linkedin: 'in/generic-wipro', loc: 'Bengaluru, India', firm: 'Wipro', delivers_training: false },
    { name: 'Karthik Subramanian', headline: 'Freelance Splunk Trainer — 14yr', linkedin: 'in/karthik-subramanian-splunk', loc: 'Hyderabad, India', firm: 'Self-employed', delivers_training: true },
  ];
  // Note: do NOT pollute markdown with query terms — that contaminates the classifier.
  // Real Apify output is just the scraped page text. Keep the mock honest.
  return fixtures.map((p, i) => ({
    url: `https://linkedin.com/${p.linkedin}`,
    metadata: { title: `${p.name} — ${p.headline}` },
    markdown: `${p.name}. ${p.headline}. Location: ${p.loc}. Firm: ${p.firm}.${p.delivers_training ? ' Delivers training programs. Conducted workshops.' : ''}${p.speaker_only ? ' Conference keynote speaker.' : ''}${p.is_institute ? ' Training institute.' : ''}`,
    _q: q,
    _mockIdx: i,
    _isInstitute: !!p.is_institute,
  }));
}

/* ---------- Apify: harvestapi LinkedIn enrichment ---------- */
// Canonicalise LinkedIn URLs to https://www.linkedin.com/in/<slug> so country-
// subdomain variants (in.linkedin.com, jo.linkedin.com, etc.) match what the
// harvestapi response uses as `url`. Without this the harvestMap lookup fails
// even when enrichment succeeded.
function canonLinkedinUrl(u) {
  if (!u) return '';
  let s = String(u).toLowerCase().split('?')[0].split('#')[0].replace(/\/$/, '');
  s = s.replace(/^https?:\/\/(?:www\.|[a-z]{2}\.)?linkedin\.com\//, 'https://www.linkedin.com/');
  return s;
}

async function enrichLinkedIn(client, urls, briefId) {
  if (process.env.MOCK_APIFY === '1' || !urls.length) {
    // Mock: derive location from URL hint. Real harvestapi returns the LinkedIn truth.
    return urls.map(u => {
      let location = 'Bengaluru, India';
      let countryCode = 'IN';
      if (u.includes('hari-babu')) { location = 'Plano, Texas, United States'; countryCode = 'US'; }
      else if (u.includes('rajesh-iyer-us') || u.includes('-us')) { location = 'Indianapolis, Indiana, United States'; countryCode = 'US'; }
      else if (u.includes('chennai')) location = 'Chennai, India';
      else if (u.includes('mumbai')) location = 'Mumbai, India';
      else if (u.includes('hyderabad')) location = 'Hyderabad, India';
      return { url: canonLinkedinUrl(u), location, countryCode, headline: '', about: '', experience: [] };
    });
  }
  try {
    // 2026-05-01: harvestapi schema changed (modifiedAt 2026-04-30):
    //   - `profileUrls` → `urls`
    //   - `profileScraperMode` is now required (specific allowed values, listed below)
    //   - response `location` is now an object `{linkedinText, countryCode, parsed:{...}}`
    //     instead of a plain string.
    const input = {
      urls,
      profileScraperMode: 'Profile details no email ($4 per 1k)',
    };
    const run = await withRetry(
      () => client.actor(APIFY_LINKEDIN_ACTOR).call(input, { waitSecs: 240 }),
      {
        maxAttempts: 3,
        baseDelayMs: 3000,
        onRetry: (attempt, delay, err) => {
          if (briefId) logAndSave(briefId, `[harvestapi retry] enrichment attempt ${attempt} failed (${(err.message || 'unknown').slice(0, 80)}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );
    if (!run || !run.defaultDatasetId) return [];
    const ds = await withRetry(
      () => client.dataset(run.defaultDatasetId).listItems(),
      { maxAttempts: 3, baseDelayMs: 1500 }
    );
    return (ds.items || []).map(it => {
      const locRaw = it.location;
      const locStr = typeof locRaw === 'string'
        ? locRaw
        : (locRaw && (locRaw.linkedinText || (locRaw.parsed && locRaw.parsed.text))) || it.locationName || '';
      const cc = (locRaw && typeof locRaw === 'object' && locRaw.countryCode)
        || (locRaw && locRaw.parsed && locRaw.parsed.countryCode)
        || '';
      return {
        url: canonLinkedinUrl(it.url || it.profileUrl || it.linkedinUrl || ''),
        location: locStr,
        countryCode: cc,
        headline: it.headline || it.title || '',
        about: it.about || it.summary || '',
        experience: it.experience || it.experiences || [],
      };
    });
  } catch (e) {
    if (briefId) logAndSave(briefId, `[harvestapi] enrichment gave up after retries: ${(e.message || 'unknown').slice(0, 100)}. Geo gate falls back to text scan — may rise NON_INDIA false rejects.`, 'warn');
    console.warn('[linkedin enrichment failed]', e.message);
    return [];
  }
}

/* ---------- Claude API (L1.1, L1.2, L2, L3, classifier) ---------- */
function getAnthropic() {
  if (_USE_CLI) return ClaudeCliClient ? new ClaudeCliClient() : null;
  if (!Anthropic || !process.env.ANTHROPIC_API_KEY) return null;
  return new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
}

async function claudeKnowledgeCall(briefId, kw, bp) {
  const client = getAnthropic();
  if (!client) return [];
  try {
    const sys = `You are a sourcing assistant for a B2B trainer placement firm in India. Given a technology/skill, return named freelance corporate trainers, independent instructors, consultants, SMEs, or architects in India who deliver training. Do NOT return conference speakers without training-delivery evidence. Do NOT return employees of: ${bp.exclusions.slice(0, 8).join(', ')}.${steeringHint(bp.steering)}`;
    const user = `Technology / skill: ${kw}\nLocation: India only.\nMode: ${bp.searchMode === 'niche' ? 'niche (rare tech, prefer founders/principals of small firms)' : 'standard'}\nReturn 5-10 named candidates with: name, LinkedIn URL (best guess if known), 1-line evidence of training-delivery (course, workshop, repeated training engagements). Format as JSON array: [{"name":"","linkedin":"","evidence":""}].`;
    const resp = await withRetry(
      () => client.messages.create({
        model: CLAUDE_SMART,
        max_tokens: 1500,
        system: sys,
        messages: [{ role: 'user', content: user }],
      }),
      {
        maxAttempts: 2,
        baseDelayMs: 4000,
        onRetry: (attempt, delay, err) => {
          if (briefId) logAndSave(briefId, `[L1.1 retry] Claude knowledge attempt ${attempt} for "${kw}" failed (status ${err.status || '?'}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );
    const text = (resp.content[0] && resp.content[0].text) || '';
    return parseJsonArray(text).map(p => ({
      url: p.linkedin || '',
      metadata: { title: `${p.name} — Claude knowledge L1.1` },
      markdown: `${p.name}. ${p.evidence || ''}. India.`,
      _q: { keyword: kw, source: 'claude_knowledge', tier: 'L1.1' },
      _claudeNote: p.evidence || '',
    })).filter(p => p.url || p.metadata.title);
  } catch (e) {
    if (briefId) logAndSave(briefId, `[L1.1] Claude knowledge gave up for "${kw}" after retries: ${(e.message || '').slice(0, 100)}. Falling back to Google-only for this keyword.`, 'warn');
    console.warn('[claude L1.1]', e.message);
    return [];
  }
}

async function claudeAdjacentTech(briefId, kw, bp) {
  const client = getAnthropic();
  if (!client) return [];
  try {
    const sys = `You suggest adjacent technologies whose practitioners often deliver training in a target technology. Concise, specific, India-trainer-pool-aware.${steeringHint(bp && bp.steering)}`;
    const user = `Target technology: ${kw}\nReturn 3 adjacent/related technologies whose Indian trainers could plausibly deliver this. JSON: ["adj1","adj2","adj3"].`;
    const resp = await withRetry(
      () => client.messages.create({
        model: CLAUDE_FAST,
        max_tokens: 200,
        system: sys,
        messages: [{ role: 'user', content: user }],
      }),
      {
        maxAttempts: 2,
        baseDelayMs: 3000,
        onRetry: (attempt, delay, err) => {
          if (briefId) logAndSave(briefId, `[L2 retry] Adjacent-tech attempt ${attempt} for "${kw}" failed (status ${err.status || '?'}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );
    const text = (resp.content[0] && resp.content[0].text) || '';
    const arr = parseJsonArray(text);
    return arr.filter(a => typeof a === 'string').slice(0, 3);
  } catch (e) {
    if (briefId) logAndSave(briefId, `[L2] Adjacent-tech gave up for "${kw}" after retries: ${(e.message || '').slice(0, 100)}.`, 'warn');
    console.warn('[claude L2]', e.message);
    return [];
  }
}

async function claudeInstitutes(briefId, kw, bp) {
  const client = getAnthropic();
  if (!client) return [];
  try {
    const sys = `You list Indian training institutes that deliver corporate training in specific technologies. Concise, verifiable.${steeringHint(bp && bp.steering)}`;
    const user = `Technology: ${kw}\nList up to 6 Indian institutes that deliver corporate training in this. JSON: [{"name":"","website":"","city":""}].`;
    const resp = await withRetry(
      () => client.messages.create({
        model: CLAUDE_FAST,
        max_tokens: 800,
        system: sys,
        messages: [{ role: 'user', content: user }],
      }),
      {
        maxAttempts: 2,
        baseDelayMs: 3000,
        onRetry: (attempt, delay, err) => {
          if (briefId) logAndSave(briefId, `[L3 retry] Institutes attempt ${attempt} for "${kw}" failed (status ${err.status || '?'}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );
    const text = (resp.content[0] && resp.content[0].text) || '';
    return parseJsonArray(text).map(i => ({
      url: i.website || '',
      metadata: { title: `${i.name} — Institute L3` },
      markdown: `${i.name}. ${i.city ? 'Located in ' + i.city + ', India' : 'India'}. Training institute.`,
      _q: { keyword: kw, source: 'claude_institutes', tier: 'L3' },
      _isInstitute: true,
    })).filter(p => p.metadata.title);
  } catch (e) {
    if (briefId) logAndSave(briefId, `[L3] Institutes gave up for "${kw}" after retries: ${(e.message || '').slice(0, 100)}.`, 'warn');
    console.warn('[claude L3]', e.message);
    return [];
  }
}

function parseJsonArray(text) {
  if (!text) return [];
  // Try fenced json first, then raw.
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  const candidate = fenced ? fenced[1] : text;
  try {
    const parsed = JSON.parse(candidate.trim());
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    // Try to extract first [...] block
    const m = candidate.match(/\[[\s\S]*\]/);
    if (m) {
      try { const p = JSON.parse(m[0]); return Array.isArray(p) ? p : []; } catch (_) {}
    }
    return [];
  }
}

/* ---------- Item normalisation ---------- */
function normaliseItem(item) {
  const url = item.url || (item.searchResult && item.searchResult.url) || '';
  const title = ((item.metadata && item.metadata.title) || (item.searchResult && item.searchResult.title) || item.title || '').trim();
  const text = (item.markdown || item.text || (item.searchResult && item.searchResult.description) || '').slice(0, 3000);
  const q = item._q || {};
  return {
    url,
    title,
    text,
    keyword: q.keyword || '',
    source: q.source || 'unknown',
    tier: q.tier || 'L?',
    isInstitute: !!item._isInstitute,
    claudeNote: item._claudeNote || '',
  };
}

function dedupeItems(items) {
  const seen = new Set();
  const out = [];
  for (const it of items) {
    const key = (it.url || it.title || '').toLowerCase().slice(0, 250);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }
  return out;
}

/* ---------- Speaker-aware classifier (the v3.1 rewrite) ---------- */
//
// New signal types:
//   TRAINER_EXPLICIT  — bio explicitly says trainer/instructor + training-delivery evidence
//   TRAINER_IMPLIED   — Founder/Consultant/Principal at a small firm, training likely
//   FREELANCE_TRAINER — explicit "freelance trainer" / "independent instructor"
//   INSTITUTE         — training firm / institute
//   SPEAKER_ONLY      — keynote/talk evidence ONLY, no training-delivery → REJECTED
//   PRACTITIONER      — engineer with skill, no teaching artefact → low rotation
//   BIG_FIRM          — employee of big SI (Wipro/Cognizant/etc) → REJECTED
//   NON_INDIA         — geo gate failed → REJECTED
//   NOISE             — irrelevant → REJECTED
//
// This is the 'manual profile check' loop from the SOP, automated.
function classifySignal(it, bp, harvest) {
  const blob = (it.title + ' ' + it.text).toLowerCase();
  const blob2 = blob + ' ' + (harvest && harvest.headline ? harvest.headline.toLowerCase() : '') + ' ' + (harvest && harvest.about ? harvest.about.toLowerCase() : '');

  // 1. Big-firm + custom + client + principal exclusion FIRST.
  // Word-boundary match so "internship at TCS" matches but "tcsworld" doesn't.
  // Skip "ex-X" / "former X" / "previously at X" — past employees are bookable
  // (they're the most common freelance trainer pool in India).
  for (const ex of bp.exclusions) {
    if (!ex) continue;
    const e = ex.toLowerCase().trim();
    if (!e) continue;
    const pat = new RegExp('\\b' + e.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'i');
    const m = pat.exec(blob2);
    if (!m) continue;
    // Look at ~30 chars before the match for "ex-", "former", "previously at",
    // "earlier worked at", "served at", "worked with"
    const before = blob2.slice(Math.max(0, m.index - 30), m.index);
    if (/(ex[-\s]|former(ly)?\s|previous(ly)?\s+(at\s+)?|earlier\s+(at|worked|with)\s+|once\s+(worked\s+)?at\s+|past\s+at\s+)\s*$/i.test(before)) {
      continue; // past employee — don't reject
    }
    return { signal: 'BIG_FIRM', reason: `Excluded company match: "${ex}"` };
  }

  // 1.5. Client-side must-NOT post-filter (belt-and-braces — Google's `-"term"` operator
  // is honoured inconsistently against LinkedIn pages, so we re-check here).
  if (bp.mustNot && bp.mustNot.length) {
    for (const term of bp.mustNot) {
      if (!term) continue;
      const t = String(term).toLowerCase().trim();
      if (!t) continue;
      // word-boundary match so "intern" doesn't match "international"
      const re = new RegExp('\\b' + t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b');
      if (re.test(blob2)) return { signal: 'MUSTNOT_HIT', reason: `Excluded by your must-NOT term: "${term}"` };
    }
  }

  // 2. Geo gate — strict via harvestapi if available, fall back to text
  const geoCheck = checkGeoIndia(blob2, harvest);
  if (!geoCheck.india) return { signal: 'NON_INDIA', reason: `Location parsed as ${geoCheck.detected}` };

  // 3. Institute signal
  if (it.isInstitute || /\b(academy|institute|bootcamp|cohort|training center|training centre|edtech|edu-?tech)\b/.test(blob2)) {
    return { signal: 'INSTITUTE', reason: 'Institute / training firm signal' };
  }

  // 4. Speaker-only check (the SOP correction). Speakers ≠ Trainers.
  const speakerKeywords = /\b(keynote|conference talk|kubecon|reinvent|talked at|spoke at|featured speaker|panelist|conference speaker)\b/;
  const trainerEvidenceKeywords = /\b(trainer|instructor|conducted training|delivered training|training programs|workshops|teaches|coaches|udemy|coursera|skillshare|course author|cohort\b)\b/;
  const isSpeaker = speakerKeywords.test(blob2);
  const hasTrainerEvidence = trainerEvidenceKeywords.test(blob2);

  if (isSpeaker && !hasTrainerEvidence) {
    return { signal: 'SPEAKER_ONLY', reason: 'Speaker / keynote evidence only — no training-delivery artefact' };
  }

  // 5. Freelance / independent trainer
  const freelance = /\b(freelance|independent|self-employed|self employed|sole proprietor|own consultancy)\b/.test(blob2);
  if (freelance && hasTrainerEvidence) {
    return { signal: 'FREELANCE_TRAINER', reason: 'Freelance + explicit training-delivery evidence' };
  }
  if (freelance && /\b(consultant|founder|principal|architect)\b/.test(blob2)) {
    return { signal: 'TRAINER_IMPLIED', reason: 'Independent consultant/founder/principal — small-firm bookable' };
  }

  // 6. Explicit trainer with delivery evidence
  if (hasTrainerEvidence) {
    return { signal: 'TRAINER_EXPLICIT', reason: 'Explicit trainer/instructor with training-delivery evidence' };
  }

  // 7. Founder/Consultant/Principal title — implied trainer for niche tech (SOP step 4)
  if (/\b(founder|principal|chief consultant|managing consultant)\b/.test(blob2) && !/\bemployee\b/.test(blob2)) {
    return { signal: 'TRAINER_IMPLIED', reason: 'Founder/Principal title — niche-domain bookable' };
  }

  // 8. Practitioner — has skill but no teaching artefact.
  // Expanded role coverage so DBAs, SREs, MLOps, security analysts, data engineers, etc. don't get NOISE-rejected.
  if (/\b(engineer|developer|architect|consultant|specialist|dba|database administrator|sre|reliability engineer|mlops|ml engineer|security engineer|security analyst|data engineer|platform engineer|sysadmin|systems administrator|devops engineer|cloud engineer|sde|swe)\b/.test(blob2)) {
    return { signal: 'PRACTITIONER', reason: 'Practitioner — has skill, no documented training delivery' };
  }

  return { signal: 'NOISE', reason: 'No clear trainer / practitioner signal' };
}

function checkGeoIndia(blob, harvest) {
  // Prefer harvestapi location field — that's the LinkedIn truth.
  if (harvest) {
    // ISO country code is the strongest signal when available
    if (harvest.countryCode === 'IN') return { india: true, detected: harvest.location || 'India (countryCode=IN)' };
    if (harvest.countryCode && harvest.countryCode !== 'IN') {
      return { india: false, detected: `${harvest.location || ''} [${harvest.countryCode}]` };
    }
    if (typeof harvest.location === 'string' && harvest.location) {
      const loc = harvest.location.toLowerCase();
      if (/\b(india|bharat)\b/.test(loc)) return { india: true, detected: harvest.location };
      for (const c of INDIAN_CITIES) if (loc.includes(c)) return { india: true, detected: harvest.location };
      if (/\b(united states|usa|uk|united kingdom|canada|australia|germany|singapore|dubai|uae|texas|california|new york|london)\b/.test(loc)) {
        return { india: false, detected: harvest.location };
      }
    }
  }
  // Fall back to text — only a strong city/India keyword passes.
  for (const c of INDIAN_CITIES) {
    const re = new RegExp(`\\b${c}\\b`, 'i');
    if (re.test(blob)) return { india: true, detected: c };
  }
  // "Based in India" — strong positive
  if (/\bbased in india\b/.test(blob)) return { india: true, detected: 'India (text)' };
  // "India" alone is too weak (Hari Babu Matta said "Indian clients")
  return { india: false, detected: '(no India location signal)' };
}

const SIGNAL_POINTS = {
  TRAINER_EXPLICIT:  40,
  FREELANCE_TRAINER: 35,
  TRAINER_IMPLIED:   25,
  INSTITUTE:         20,
  PRACTITIONER:       8,
  SPEAKER_ONLY:       0, // rejected
  BIG_FIRM:           0, // rejected
  NON_INDIA:          0, // rejected
  MUSTNOT_HIT:        0, // rejected — operator-specified term
  NOISE:              0, // rejected
};

const REJECTED_SIGNALS = new Set(['SPEAKER_ONLY', 'BIG_FIRM', 'NON_INDIA', 'MUSTNOT_HIT', 'NOISE']);

function bucketFit(it, keywords) {
  const blob = (it.title + ' ' + it.text).toLowerCase();
  let hits = 0;
  for (const kw of keywords) {
    const re = new RegExp(kw.toLowerCase().replace(/[^\w\s]/g, '.'), 'g');
    const m = blob.match(re);
    if (m) hits += m.length;
  }
  if (hits >= 4) return 3;
  if (hits >= 2) return 2;
  if (hits >= 1) return 1;
  return 0;
}

function verifyScore(it, harvest) {
  const hasLinkedIn = it.url && it.url.includes('linkedin.com/in/');
  const hasHarvest = harvest && (harvest.location || harvest.headline);
  const hasSecondSource = it.tier === 'L1.1' || it.tier === 'L1.3' || it.text.length > 500;
  if (hasLinkedIn && hasHarvest && hasSecondSource) return 3;
  if (hasLinkedIn && (hasHarvest || hasSecondSource)) return 2;
  if (it.url) return 1;
  return 0;
}

function bookScore(signal) {
  return ({
    INSTITUTE: 3,
    FREELANCE_TRAINER: 3,
    TRAINER_EXPLICIT: 2,
    TRAINER_IMPLIED: 2,
    PRACTITIONER: 1,
  })[signal] || 0;
}

function compositeScore(p, weights) {
  if (REJECTED_SIGNALS.has(p.signal)) return 0;
  const w = weights || { signal: 40, bucket: 30, verify: 15, book: 15 };
  // Normalize weights to 100 max if the searcher tweaked them
  const total = w.signal + w.bucket + w.verify + w.book;
  const norm = total > 0 ? 100 / total : 1;
  const sigPts = (SIGNAL_POINTS[p.signal] / 40) * w.signal;
  const bktPts = (p.bucketFit / 3) * w.bucket;
  const verPts = (p.verify / 3) * w.verify;
  const bkPts  = (p.book / 3) * w.book;
  return Math.round((sigPts + bktPts + verPts + bkPts) * norm);
}

/* ---------- Reason-traceable accept/reject ---------- */
function buildDecision(p, bp) {
  if (REJECTED_SIGNALS.has(p.signal)) {
    return {
      decision: 'reject',
      decision_reason: p.reason || 'Rejected — no trainer evidence',
      decision_url: p.url || '',
      decision_snippet: (p.text || '').slice(0, 240),
    };
  }
  return {
    decision: 'accept',
    decision_reason: p.reason || 'Trainer evidence present',
    decision_url: p.url || '',
    decision_snippet: (p.text || '').slice(0, 240),
  };
}

/* ---------- Excel ---------- */
async function buildXlsx(brief, accepted, rejected, bp, timings) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'RJP Sourcing Portal v3.1';
  wb.created = new Date();

  // 1. Candidates
  const ws = wb.addWorksheet('Candidates');
  ws.columns = [
    { header: 'Rank', key: 'rank', width: 6 },
    { header: 'Score', key: 'score', width: 7 },
    { header: 'Name / Title', key: 'title', width: 50 },
    { header: 'LinkedIn / URL', key: 'url', width: 45 },
    { header: 'Signal', key: 'signal', width: 18 },
    { header: 'Decision reason', key: 'why', width: 40 },
    { header: 'Source URL (verifiable)', key: 'src', width: 40 },
    { header: 'Source snippet', key: 'snip', width: 60 },
    { header: 'Bucket fit', key: 'bf', width: 9 },
    { header: 'Verify', key: 'v', width: 7 },
    { header: 'Book', key: 'b', width: 6 },
    { header: 'Tier', key: 'tier', width: 8 },
    { header: 'Keyword', key: 'kw', width: 22 },
  ];
  ws.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  ws.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E40AF' } };

  accepted.forEach((c, i) => {
    ws.addRow({
      rank: i + 1,
      score: c.score,
      title: c.title || '(no title)',
      url: c.url || '',
      signal: c.signal,
      why: c.decision_reason,
      src: c.decision_url || c.url || '',
      snip: c.decision_snippet || '',
      bf: c.bucketFit,
      v: c.verify,
      b: c.book,
      tier: c.tier,
      kw: c.keyword,
    });
  });

  for (let r = 2; r <= ws.rowCount; r++) {
    const s = ws.getCell('B' + r).value;
    let fill;
    if (s >= 85) fill = 'FFE8F3EC';
    else if (s >= 65) fill = 'FFEBF0F7';
    else if (s >= 40) fill = 'FFFBF2E1';
    else fill = 'FFF6E6E8';
    ws.getCell('B' + r).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: fill } };
    ws.getRow(r).alignment = { vertical: 'top', wrapText: true };
  }

  // 2. Rejected — for audit, the 'why' per the verifiable-conclusion ask
  const wsR = wb.addWorksheet('Rejected (audit)');
  wsR.columns = [
    { header: 'Title', key: 'title', width: 45 },
    { header: 'URL', key: 'url', width: 40 },
    { header: 'Signal', key: 'signal', width: 16 },
    { header: 'Rejection reason', key: 'why', width: 50 },
    { header: 'Source snippet', key: 'snip', width: 60 },
    { header: 'Tier', key: 'tier', width: 8 },
    { header: 'Keyword', key: 'kw', width: 22 },
  ];
  wsR.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  wsR.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6B7280' } };
  rejected.forEach(c => {
    wsR.addRow({
      title: c.title || '(no title)',
      url: c.url || '',
      signal: c.signal,
      why: c.decision_reason,
      snip: c.decision_snippet || '',
      tier: c.tier,
      kw: c.keyword,
    });
  });

  // 3. Brief context
  const wsCtx = wb.addWorksheet('Brief context');
  wsCtx.columns = [{ header: 'Field', key: 'k', width: 26 }, { header: 'Value', key: 'v', width: 80 }];
  wsCtx.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  wsCtx.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E40AF' } };
  const rows = [
    ['Brief ID', brief.id],
    ['Title', brief.title],
    ['Mode', bp.searchMode === 'niche' ? 'Niche' : 'Standard'],
    ['Keywords', bp.keywords.join('  |  ')],
    ['Must include', bp.must.join('  |  ') || '—'],
    ['Should include', bp.should.join('  |  ') || '—'],
    ['Must NOT include', bp.mustNot.join('  |  ') || '—'],
    ['Client company (excluded)', bp.clientCompany || '—'],
    ['Client principal (excluded)', bp.clientPrincipal || '—'],
    ['Custom exclusions', bp.customExclusions.join('  |  ') || '—'],
    ['Default big-firm exclusions', DEFAULT_BIGFIRM_EXCLUSIONS.join(', ')],
    ['Geo', brief.geo || 'India'],
    ['Submitted at', brief.createdAt],
    ['Submitted by', (brief.operator && brief.operator.name) || brief.submittedBy || '—'],
    ['Steering / direction', brief.steering || '—'],
    ['', ''],
    ['STAGE TIMINGS (TAT)', ''],
    ...timings.map(t => [t.stage, `${t.elapsed}s`]),
    ['', ''],
    ['SCORING WEIGHTS (searcher-tweakable)', ''],
    ['Signal',       `0–${bp.advanced.weights.signal}`],
    ['Bucket fit',   `0–${bp.advanced.weights.bucket}`],
    ['Verify',       `0–${bp.advanced.weights.verify}`],
    ['Book',         `0–${bp.advanced.weights.book}`],
    ['', ''],
    ['SIGNAL DEFINITIONS', ''],
    ['TRAINER_EXPLICIT',  'Bio explicitly says trainer/instructor + delivery evidence (max signal)'],
    ['FREELANCE_TRAINER', 'Independent + explicit training-delivery'],
    ['TRAINER_IMPLIED',   'Founder/Principal/Consultant at small firm — niche-domain bookable'],
    ['INSTITUTE',         'Training institute / firm'],
    ['PRACTITIONER',      'Has skill, no teaching artefact'],
    ['SPEAKER_ONLY',      'Conference/keynote ONLY → REJECTED (per SOP)'],
    ['BIG_FIRM',          'Tech M / Wipro / etc / client / principal → REJECTED'],
    ['NON_INDIA',         'Location not India → REJECTED'],
    ['MUSTNOT_HIT',       'Hit one of your must-NOT terms → REJECTED'],
  ];
  rows.forEach(r => wsCtx.addRow({ k: r[0], v: r[1] }));
  wsCtx.eachRow(r => r.alignment = { vertical: 'top', wrapText: true });

  const file = path.join(OUTPUT_DIR, brief.id + '.xlsx');
  await wb.xlsx.writeFile(file);
  return file;
}

/* ---------- Main pipeline ---------- */
async function runPipeline(briefId, opts = {}) {
  const brief = store.get(briefId);
  if (!brief) return;
  const apifyToken = process.env.APIFY_TOKEN;
  if (!apifyToken) {
    store.update(briefId, { status: 'failed', error: 'APIFY_TOKEN not set' });
    logAndSave(briefId, 'APIFY_TOKEN missing on server', 'err');
    return;
  }
  const previewMode = !!opts.preview;
  const t0 = Date.now();
  const timings = [];
  const stageStart = (name) => {
    const s = Date.now();
    return () => timings.push({ stage: name, elapsed: ((Date.now() - s) / 1000).toFixed(1) });
  };

  try {
    const bp = normalizeBrief(brief);
    store.update(briefId, {
      status: previewMode ? 'preview' : 'discovery',
      counts: {},
      mode: bp.searchMode,
      tatTarget: bp.searchMode === 'niche' ? 720 : 480,
    });
    logAndSave(briefId, `Pipeline starting — ${bp.searchMode.toUpperCase()} mode, ${bp.keywords.length} keyword(s)${previewMode ? ' [PREVIEW]' : ''}`);
    logAndSave(briefId, `Exclusions in effect: ${bp.exclusions.length} entries (default big-firms + custom + client/principal)`);

    const apifyClient = new ApifyClient({ token: apifyToken });

    // ---- L1.1: Claude knowledge ----
    let allItems = [];
    if (!previewMode && hasLlmClient()) {
      const stop = stageStart('L1.1 Claude knowledge');
      for (const kw of bp.keywords) {
        const items = await claudeKnowledgeCall(briefId, kw, bp);
        logAndSave(briefId, `[L1.1] Claude knowledge for "${kw}" → ${items.length} candidates`);
        allItems = allItems.concat(items);
      }
      stop();
    } else if (!previewMode) {
      logAndSave(briefId, `L1.1 skipped — no LLM client (set ANTHROPIC_VIA_CLAUDE_CLI=true or ANTHROPIC_API_KEY). Falling back to Google-only.`, 'warn');
    }

    // ---- L1.3: Apify Google ----
    const stopGoogle = stageStart('L1.3 Google search');
    const queries = buildGoogleQueries(brief, bp);
    const queriesToRun = previewMode ? queries.slice(0, 1) : queries;
    logAndSave(briefId, `Built ${queries.length} queries; running ${queriesToRun.length}${previewMode ? ' (preview)' : ''}`);
    for (let i = 0; i < queriesToRun.length; i++) {
      const q = queriesToRun[i];
      logAndSave(briefId, `[L1.3 ${i + 1}/${queriesToRun.length}] ${q.query.slice(0, 110)}`);
      const items = await runGoogleQuery(apifyClient, q, briefId);
      logAndSave(briefId, `[L1.3 ${i + 1}/${queriesToRun.length}] +${items.length} items`);
      allItems = allItems.concat(items);
      store.update(briefId, { counts: { ...((store.get(briefId) || {}).counts || {}), discovered: allItems.length } });
    }
    stopGoogle();

    // ---- L2: adjacent-tech expansion (Niche or thin results) ----
    let normalised = dedupeItems(allItems.map(normaliseItem));
    if (!previewMode && (bp.searchMode === 'niche' || normalised.length < 10) && hasLlmClient()) {
      const stop = stageStart('L2 Adjacent tech');
      for (const kw of bp.keywords) {
        const adj = await claudeAdjacentTech(briefId, kw, bp);
        logAndSave(briefId, `[L2] Adjacent tech for "${kw}": ${adj.join(', ') || '(none)'}`);
        for (const a of adj) {
          // Run a single LinkedIn query per adjacent tech
          const q = { query: `independent instructor ${a} India site:linkedin.com/in`, keyword: a, source: 'linkedin', tier: 'L2' };
          const items = await runGoogleQuery(apifyClient, q, briefId);
          logAndSave(briefId, `[L2] "${a}" → +${items.length} items`);
          allItems = allItems.concat(items);
        }
      }
      normalised = dedupeItems(allItems.map(normaliseItem));
      stop();
    }

    // ---- L3: Institutes (Niche + still thin) ----
    if (!previewMode && bp.searchMode === 'niche' && normalised.length < 8 && hasLlmClient()) {
      const stop = stageStart('L3 Institutes');
      for (const kw of bp.keywords) {
        const inst = await claudeInstitutes(briefId, kw, bp);
        logAndSave(briefId, `[L3] Institutes for "${kw}" → ${inst.length}`);
        allItems = allItems.concat(inst);
      }
      normalised = dedupeItems(allItems.map(normaliseItem));
      stop();
    }

    logAndSave(briefId, `${normalised.length} unique items across all tiers`);

    // ---- LinkedIn enrichment via harvestapi ----
    // Cap at 60: harvestapi charges ~$4/1k profiles ($0.24 per brief at the cap),
    // and master-test 2026-05-01 showed that a 30-URL cap left half of LinkedIn
    // hits unenriched, which collapsed to weak text-only geo classification and
    // false-rejected Indian-named trainers as NON_INDIA. Keep the cap so a runaway
    // brief can't enrich 1000 profiles in one shot.
    const ENRICH_CAP = parseInt(process.env.LINKEDIN_ENRICH_CAP || '60', 10);
    const stopEnrich = stageStart('LinkedIn enrichment');
    const linkedInUrls = normalised.map(n => n.url).filter(u => u && u.includes('linkedin.com/in/')).slice(0, ENRICH_CAP);
    let harvestMap = {};
    if (linkedInUrls.length && bp.advanced.sources.linkedin && !previewMode) {
      const harvested = await enrichLinkedIn(apifyClient, linkedInUrls, briefId);
      // Index by canonical URL — harvestapi normalises in.linkedin.com → www.linkedin.com,
      // so the input URL and the response URL won't byte-match without canonLinkedinUrl.
      harvestMap = Object.fromEntries(harvested.map(h => [canonLinkedinUrl(h.url), h]));
      logAndSave(briefId, `[harvestapi] enriched ${harvested.length} LinkedIn profiles`);
    }
    stopEnrich();

    // ---- Scoring + classification ----
    store.update(briefId, { status: 'scoring' });
    const stopScore = stageStart('Scoring + classification');
    const scored = normalised.map(it => {
      const harvest = harvestMap[canonLinkedinUrl(it.url)] || null;
      const cls = classifySignal(it, bp, harvest);
      const bf = bucketFit(it, bp.keywords);
      const v = verifyScore(it, harvest);
      const bk = bookScore(cls.signal);
      const candidate = {
        ...it,
        signal: cls.signal,
        reason: cls.reason,
        bucketFit: bf,
        verify: v,
        book: bk,
        score: 0,
      };
      candidate.score = compositeScore(candidate, bp.advanced.weights);
      const decision = buildDecision(candidate, bp);
      return { ...candidate, ...decision };
    });

    const accepted = scored.filter(c => c.decision === 'accept').sort((a, b) => b.score - a.score);
    const rejected = scored.filter(c => c.decision === 'reject');

    // Apply quality cap
    const capped = accepted.slice(0, bp.advanced.qualityCap);
    stopScore();

    logAndSave(briefId, `Scored ${scored.length}: ${accepted.length} accepted (capped to ${capped.length}), ${rejected.length} rejected`);

    // ---- Preview return without xlsx ----
    if (previewMode) {
      const sample = capped.slice(0, PREVIEW_SAMPLE_SIZE);
      const totalElapsed = elapsedSec(t0);
      store.update(briefId, {
        status: 'preview_ready',
        previewSample: sample,
        previewRejected: rejected.slice(0, 5),
        previewElapsed: totalElapsed,
      });
      logAndSave(briefId, `Preview ready in ${totalElapsed}s — ${sample.length} sample profiles`);
      return;
    }

    // ---- Build xlsx ----
    store.update(briefId, { status: 'packaging' });
    const stopBuild = stageStart('Excel build');
    const file = await buildXlsx(brief, capped, rejected, bp, timings);
    stopBuild();
    const stat = fs.statSync(file);
    const totalElapsed = elapsedSec(t0);

    store.update(briefId, {
      status: 'complete',
      outputFile: path.basename(file),
      counts: { discovered: normalised.length, accepted: capped.length, rejected: rejected.length, scored: scored.length },
      completedAt: new Date().toISOString(),
      timings,
      totalElapsed,
      acceptedSample: capped.slice(0, 10),
      rejectedSample: rejected.slice(0, 10),
    });
    logAndSave(briefId, `Pipeline complete — ${capped.length} candidates, ${(stat.size / 1024).toFixed(1)} KB, ${totalElapsed}s total`);
  } catch (e) {
    console.error('[pipeline error]', e);
    logAndSave(briefId, 'Pipeline failed: ' + e.message, 'err');
    store.update(briefId, { status: 'failed', error: e.message });
  }
}

/* ---------- Re-run with feedback (Form 1) ---------- */
async function runWithFeedback(briefId, feedbackText) {
  const brief = store.get(briefId);
  if (!brief) return;
  // Append feedback to steering, bump revision number, re-run
  const rev = (brief.feedbackRevisions || 0) + 1;
  const newSteering = (brief.steering || '') + `\n\n--- Feedback revision ${rev} (${new Date().toISOString()}) ---\n` + (feedbackText || '');
  store.update(briefId, {
    steering: newSteering,
    feedbackRevisions: rev,
    feedbackHistory: (brief.feedbackHistory || []).concat([{ ts: new Date().toISOString(), text: feedbackText, rev }]),
    status: 'queued',
    log: (brief.log || []).concat([{ ts: new Date().toISOString(), msg: `Feedback rev ${rev} applied — re-running`, kind: 'info' }]),
  });
  return runPipeline(briefId);
}

module.exports = { runPipeline, runWithFeedback, OUTPUT_DIR, DEFAULT_BIGFIRM_EXCLUSIONS };
