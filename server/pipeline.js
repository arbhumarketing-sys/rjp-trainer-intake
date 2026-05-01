/**
 * RJP Sourcing Pipeline — v3.2
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
  // Pull persistent exclusions from store. Used so RJP doesn't have to retype
  // "InfraCloud", "AnalyticsVidhya" etc. on every brief once they've added
  // them to the always-exclude list. See store.getPersistentExclusions.
  const persistentExclusions = (typeof store.getPersistentExclusions === 'function'
    ? store.getPersistentExclusions().map(e => e.term)
    : []);
  const exclusions = dedupeLower([
    ...DEFAULT_BIGFIRM_EXCLUSIONS,
    ...persistentExclusions,
    ...(clientCompany ? [clientCompany] : []),
    ...(clientPrincipal ? [clientPrincipal] : []),
    ...customExclusions,
  ]);
  // Steering — operator's free-text direction + accumulated Form-1 feedback. Capped
  // so multi-revision feedback histories don't blow the LLM prompt budget. Latest
  // feedback is always at the end (runWithFeedback appends), so tail-slice.
  const steering = (brief.steering || '').slice(-2500);
  // v3.2: pass the brief's domain through so claudeKnowledgeCall etc. can
  // append domain-specific tuning to the system prompt.
  const domain = (brief.domain || '').trim();
  return { keywords, must, should, mustNot, clientCompany, clientPrincipal, customExclusions, searchMode, advanced, exclusions, steering, domain };
}

/* ---------- Domain-specific prompt tuning (v3.2) ---------- */
// Different domains have predictable Indian trainer-pool patterns. A generic
// "list trainers for X" prompt misses those patterns. Adding a 1-2 sentence
// domain hint to the L1.1/L2/L3 system prompts steers Claude toward the
// canonical trainer pool for the technology, lifting recall significantly.
const DOMAIN_TUNING = {
  'salesforce':       'Look for OmniStudio, Service/Sales/Marketing Cloud, MuleSoft specialists. Common India pool: independent consultants who left Cognizant/Wipro Salesforce practices.',
  'aws':              'Look for AWS-certified architects (especially Solutions Architect Pro). Common pool: ex-AWS sales engineers turned freelance consultants, re:Invent attendees.',
  'azure':            'Look for Azure Solutions Architect Expert / DevOps Engineer Expert holders. Common pool: ex-Microsoft FTEs and Microsoft Most Valuable Professionals (MVPs) in India.',
  'gcp':              'Look for Google Cloud Professional Architect / Data Engineer holders. Smaller pool than AWS — prefer Google Cloud Champion Innovators.',
  'kubernetes':       'Look for CKA/CKAD/CKS-certified engineers. Pool: CNCF Ambassadors who actually run cohort programs (NOT just KubeCon speakers — that was the v3.1 SOP correction).',
  'sap':              'Look for module specialists (FICO, MM, SD, HCM, S/4HANA). Common pool: ex-SAP labs Bengaluru employees who went freelance.',
  'data science':     'Look for ML/data engineers, Kaggle masters who teach. Pool: AnalyticsVidhya / InsofE / GreatLearning alumni who went independent.',
  'data':             'Cover both data engineering and ML — pool overlaps with practitioner-trainers from product analytics teams.',
  'cybersecurity':    'Look for OSCP/CISSP/OSCE-certified pentesters/architects. Pool: ex-Big-4 (Deloitte/EY/PwC/KPMG) security consultants and OWASP chapter leads.',
  'java':             'Pool is large; prioritise Spring/microservices/JVM-tuning specialists. Senior architects with 10+ yrs preferred.',
  'python':           'Look for Python web (Django/FastAPI/Flask) or scientific-computing teachers. Differentiate from data-science overlap.',
  'devops':           'Look for engineers with multi-tool depth (Terraform + K8s + CI/CD). Pool: ex-startup SREs and DevOps Bengaluru meetup organisers.',
  'backstage':        'Very thin pool in India. Consider InfraCloud + ex-Spotify-style platform-engineering practitioners. May need adjacent-tech (developer portals, internal developer platforms) expansion.',
};

function domainHint(domain) {
  if (!domain) return '';
  const d = String(domain).toLowerCase();
  for (const [key, hint] of Object.entries(DOMAIN_TUNING)) {
    if (d.includes(key)) return `\n\nDOMAIN PATTERN (${key}): ${hint}`;
  }
  return '';
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
// v3.2 reorder: instead of "all LinkedIn variants per keyword, then others",
// interleave one query per source type per keyword first, then fill the
// remaining query budget with extra LinkedIn variants. Reason: with
// multi-keyword briefs the old order would consume the entire query cap on
// LinkedIn alone, and YouTube/Udemy/marketplace/blog queries (especially the
// new L4 tier) would never fire. Interleaved order ensures every source
// gets at least one shot per keyword before LinkedIn fills the slack.
function buildGoogleQueries(brief, bp) {
  const sites = bp.advanced.sources;
  const trainerVariants = ['independent instructor', 'freelance trainer', 'corporate trainer', 'instructor', 'trainer'];

  // Build per-keyword: one query per source-type, plus the LinkedIn variant array
  const primaryByKw = [];     // 1 LinkedIn (primary variant) + 1 marketplace + 1 blog + 1 youtube + 1 udemy
  const extraLinkedinByKw = []; // remaining LinkedIn variants

  for (const kw of bp.keywords) {
    const kwClean = sanitizeQueryTerm(kw);
    const must     = bp.must.map(m => sanitizeQueryTerm(m)).filter(Boolean).map(m => `"${m}"`).join(' ');
    const should   = bp.should.map(s => sanitizeQueryTerm(s)).filter(Boolean).map(s => `"${s}"`).join(' OR ');
    const mustNot  = bp.mustNot.map(m => sanitizeQueryTerm(m)).filter(Boolean).map(m => `-"${m}"`).join(' ');
    const exclusions = bp.exclusions.slice(0, 5).map(e => `-"${e}"`).join(' ');

    const perKw = [];
    if (sites.linkedin) {
      // Primary: most generic high-recall variant first
      perKw.push({
        query: `${trainerVariants[0]} ${kwClean} India site:linkedin.com/in ${must} ${should ? '(' + should + ')' : ''} ${mustNot} ${exclusions}`.replace(/\s+/g, ' ').trim(),
        keyword: kw, variant: trainerVariants[0], source: 'linkedin', tier: 'L1.3',
      });
    }
    if (sites.urbanpro) {
      perKw.push({
        query: `${kwClean} trainer India site:urbanpro.com OR site:sulekha.com`,
        keyword: kw, variant: 'marketplace', source: 'urbanpro', tier: 'L1.3',
      });
    }
    if (sites.blogs) {
      perKw.push({
        query: `${kwClean} freelance trainer India -site:linkedin.com`,
        keyword: kw, variant: 'blog', source: 'blogs', tier: 'L1.3',
      });
    }
    if (sites.youtube) {
      // L4 tier: tutorials/channels (finds creators who teach but don't self-label as "trainer")
      perKw.push({
        query: `${kwClean} tutorial India site:youtube.com`,
        keyword: kw, variant: 'youtube-channel', source: 'youtube', tier: 'L4',
      });
    }
    if (sites.udemy) {
      // L4 tier: Udemy instructor pages carry course-count + student-count signals
      perKw.push({
        query: `${kwClean} course India site:udemy.com`,
        keyword: kw, variant: 'udemy-instructor', source: 'udemy', tier: 'L4',
      });
    }
    primaryByKw.push(perKw);

    // Extra LinkedIn variants (fill remaining budget)
    if (sites.linkedin) {
      for (let v = 1; v < trainerVariants.length; v++) {
        const variant = trainerVariants[v];
        extraLinkedinByKw.push({
          query: `${variant} ${kwClean} India site:linkedin.com/in ${must} ${should ? '(' + should + ')' : ''} ${mustNot} ${exclusions}`.replace(/\s+/g, ' ').trim(),
          keyword: kw, variant, source: 'linkedin', tier: 'L1.3',
        });
      }
    }
  }

  // Flatten primary first (per-keyword grouped), then fill with extra LinkedIn variants
  const ordered = [];
  for (const perKw of primaryByKw) ordered.push(...perKw);
  ordered.push(...extraLinkedinByKw);
  return ordered.slice(0, bp.advanced.queryDepth);
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
    const sys = `You are a sourcing assistant for a B2B trainer placement firm in India. Given a technology/skill, return named freelance corporate trainers, independent instructors, consultants, SMEs, or architects in India who deliver training. Do NOT return conference speakers without training-delivery evidence. Do NOT return employees of: ${bp.exclusions.slice(0, 8).join(', ')}.${domainHint(bp.domain)}${steeringHint(bp.steering)}`;
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
    const sys = `You suggest adjacent technologies whose practitioners often deliver training in a target technology. Concise, specific, India-trainer-pool-aware.${domainHint(bp && bp.domain)}${steeringHint(bp && bp.steering)}`;
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
    const sys = `You list Indian training institutes that deliver corporate training in specific technologies. Concise, verifiable.${domainHint(bp && bp.domain)}${steeringHint(bp && bp.steering)}`;
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

// v3.2: replaces simple dedupe with source-merging. Same identity (URL or
// title) appearing in multiple tiers/sources = stronger signal. We keep the
// richest copy (longest text, most metadata) and tag it with the count of
// distinct sources for compositeScore to award a bonus. Same effective dedupe
// behaviour for downstream consumers — just no longer discards information.
function mergeBySource(items) {
  const groups = new Map();
  for (const it of items) {
    const key = (it.url || it.title || '').toLowerCase().slice(0, 250);
    if (!key) continue;
    const sourceTag = it.tier && it.source ? `${it.tier}:${it.source}` : (it.tier || it.source || 'unknown');
    if (!groups.has(key)) {
      groups.set(key, { ...it, _sources: new Set([sourceTag]) });
    } else {
      const merged = groups.get(key);
      merged._sources.add(sourceTag);
      // Prefer the entry with more text
      if ((it.text || '').length > (merged.text || '').length) {
        merged.text = it.text;
      }
      // Prefer non-empty title
      if (it.title && (!merged.title || it.title.length > merged.title.length)) {
        merged.title = it.title;
      }
      // L1.1 (Claude knowledge) carries narrative evidence — keep it if found later
      if (it.claudeNote && !merged.claudeNote) merged.claudeNote = it.claudeNote;
    }
  }
  return Array.from(groups.values()).map(it => {
    const sources = Array.from(it._sources);
    delete it._sources;
    return { ...it, multiSource: sources.length, sources };
  });
}

// Backwards-compat alias — older code paths could still call dedupeItems.
const dedupeItems = mergeBySource;

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
// As of v3.2 this is the FALLBACK classifier — the primary path is
// classifySignalsBatched which calls Haiku on batches of 15 profiles.
// Fallback is used when:
//   - hasLlmClient() returns false (no API key, no CLI token)
//   - DISABLE_LLM_CLASSIFIER env var is set to '1'
//   - the LLM batch call fails after retries (per-batch fallback)
//   - the LLM returns an unknown signal name (per-profile fallback)
// The 9-class taxonomy is preserved, so output shape is identical.
function classifySignalRegex(it, bp, harvest) {
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

/* ---------- LLM-based classifier (v3.2 — primary path, regex is fallback) ---------- */
//
// Replaces per-profile regex matching with batched Haiku calls. The regex
// classifier (classifySignalRegex) is preserved as a fallback for:
//   - environments with no LLM client (no API key, no CLI token)
//   - explicit operator opt-out (DISABLE_LLM_CLASSIFIER=1)
//   - per-batch failure after retries
//   - LLM hallucinated signal name not in the 9-class taxonomy
//
// Why batched: the claude CLI subprocess has ~6-9s init overhead per call.
// Per-profile would be 6-9s × 50 profiles = 5-7 min, unacceptable for the
// 8-12 min TAT target. Batching 15 profiles per call brings classification
// down to ~15-20s total. Cost is ~$0.01/brief (Haiku is cheap).
//
// Quality gain over regex: catches "Founder of small Salesforce consultancy
// who runs monthly bootcamps" (was PRACTITIONER), "ex-Wipro freelance trainer"
// (was BIG_FIRM via regex word-boundary noise), nuanced INSTITUTE vs
// FREELANCE_TRAINER distinctions, semi-implicit training claims, etc.

const LLM_CLASSIFIER_BATCH_SIZE = parseInt(process.env.LLM_CLASSIFIER_BATCH_SIZE || '15', 10);
const LLM_CLASSIFIER_CONCURRENCY = parseInt(process.env.LLM_CLASSIFIER_CONCURRENCY || '3', 10);
const VALID_SIGNALS = new Set(Object.keys(SIGNAL_POINTS));

async function classifySignalsBatched(itemsWithHarvest, bp, briefId) {
  const useLlm = hasLlmClient() && process.env.DISABLE_LLM_CLASSIFIER !== '1';
  if (!useLlm) {
    if (briefId) logAndSave(briefId, `Classifier: regex mode (LLM disabled or unavailable)`);
    return itemsWithHarvest.map(({ it, harvest }) => classifySignalRegex(it, bp, harvest));
  }

  // v3.2 (post-E2E observation): each Claude CLI batch takes ~50s due to
  // subprocess startup + API latency, NOT the ~9s originally estimated. With
  // 100+ candidates and 7-10 batches, sequential classification took 5-8 min.
  // Parallel worker pool with concurrency=3 cuts that to 2-3 min. Memory
  // headroom on Render free tier is fine (~50MB per CLI subprocess, base
  // Node ~150MB, total ~300MB of 512MB cap).
  const batches = [];
  for (let i = 0; i < itemsWithHarvest.length; i += LLM_CLASSIFIER_BATCH_SIZE) {
    batches.push({ offset: i, items: itemsWithHarvest.slice(i, i + LLM_CLASSIFIER_BATCH_SIZE) });
  }
  const concurrency = Math.max(1, Math.min(LLM_CLASSIFIER_CONCURRENCY, batches.length));
  if (briefId) logAndSave(briefId, `Classifier: LLM mode (Haiku), ${itemsWithHarvest.length} profiles in ${batches.length} batch(es) of ${LLM_CLASSIFIER_BATCH_SIZE}, concurrency=${concurrency}`);

  const results = new Array(itemsWithHarvest.length);
  let nextBatch = 0;
  const worker = async () => {
    while (true) {
      const bidx = nextBatch++;
      if (bidx >= batches.length) return;
      const { offset, items } = batches[bidx];
      let batchResults;
      try {
        batchResults = await classifySignalLLM(items, bp, briefId, offset);
      } catch (e) {
        if (briefId) logAndSave(briefId, `[classifier] LLM batch ${offset}-${offset + items.length} failed (${(e.message || '').slice(0, 80)}); falling back to regex for these ${items.length} profile(s)`, 'warn');
        batchResults = items.map(({ it, harvest }) => classifySignalRegex(it, bp, harvest));
      }
      for (let j = 0; j < items.length; j++) {
        // Per-profile fallback: if LLM returned an unknown signal or missing entry, regex it.
        const cls = batchResults[j];
        if (cls && VALID_SIGNALS.has(cls.signal)) {
          results[offset + j] = cls;
        } else {
          results[offset + j] = classifySignalRegex(items[j].it, bp, items[j].harvest);
        }
      }
    }
  };
  await Promise.all(Array.from({ length: concurrency }, worker));
  return results;
}

async function classifySignalLLM(batch, bp, briefId, batchOffset) {
  const client = getAnthropic();
  if (!client) throw new Error('No LLM client available');

  const sys = `You classify LinkedIn-style profiles for an Indian B2B trainer placement firm into ONE signal category each.

Categories (use EXACT strings):
  TRAINER_EXPLICIT  — bio EXPLICITLY says trainer/instructor + has training-delivery evidence (workshops, courses, cohorts, "delivered training")
  FREELANCE_TRAINER — independent/freelance/self-employed + explicit training-delivery
  TRAINER_IMPLIED   — Founder/Principal/Consultant at small firm; niche-domain bookable as trainer
  INSTITUTE         — training institute/academy/bootcamp/edtech
  PRACTITIONER      — has the skill, but NO teaching artefact (engineer/dev/architect/SRE/DBA/etc.)
  SPEAKER_ONLY      — keynote/conference talks ONLY, no training-delivery → REJECTED
  BIG_FIRM          — CURRENT employee of an excluded company (see list) → REJECTED
  NON_INDIA         — location is NOT India → REJECTED
  MUSTNOT_HIT       — bio hits one of the operator's must-NOT terms → REJECTED
  NOISE             — irrelevant to the brief → REJECTED

CRITICAL RULES:
  1. Conference speakers without training-delivery evidence are SPEAKER_ONLY, not trainers.
  2. Big-firm rule applies only to CURRENT employees. "ex-X", "former X", "previously at X", "earlier at X" → NOT BIG_FIRM (past employees are bookable trainers).
  3. Geo: trust the countryCode field. If countryCode is non-IN, classify as NON_INDIA.
  4. PRACTITIONER means skilled but no teaching evidence — it's not a rejection, just a low-scoring accept.
  5. If the profile data is too sparse to decide, use NOISE.

EXCLUDED COMPANIES (current employees → BIG_FIRM): ${bp.exclusions.slice(0, 14).join(', ')}
MUST-NOT TERMS (any hit → MUSTNOT_HIT): ${(bp.mustNot || []).join(', ') || '(none)'}
${steeringHint(bp.steering)}

Output: ONLY a JSON array, one entry per profile, in INPUT ORDER.
Format: [{"i":0,"signal":"TRAINER_EXPLICIT","reason":"≤10 word reason"}, ...]`;

  const profilesText = batch.map(({ it, harvest }, idx) => {
    const lines = [`Profile ${idx}:`, `  Title: ${(it.title || '(none)').slice(0, 220)}`];
    if (harvest) {
      if (harvest.headline) lines.push(`  Headline: ${harvest.headline.slice(0, 220)}`);
      if (harvest.location || harvest.countryCode) lines.push(`  Location: ${harvest.location || '?'} (countryCode: ${harvest.countryCode || '?'})`);
      if (harvest.about) lines.push(`  About: ${harvest.about.slice(0, 380)}`);
    } else {
      lines.push(`  Location: (no LinkedIn enrichment available — geo from text only)`);
    }
    if (it.text) lines.push(`  Snippet: ${it.text.slice(0, 380)}`);
    if (it.tier) lines.push(`  Source tier: ${it.tier}`);
    return lines.join('\n');
  }).join('\n\n');

  const user = `Classify these ${batch.length} profile(s):\n\n${profilesText}\n\nReturn JSON array now.`;

  const resp = await withRetry(
    () => client.messages.create({
      model: CLAUDE_FAST,
      max_tokens: Math.max(800, batch.length * 80),
      system: sys,
      messages: [{ role: 'user', content: user }],
    }),
    {
      maxAttempts: 2,
      baseDelayMs: 4000,
      onRetry: (attempt, delay, err) => {
        if (briefId) logAndSave(briefId, `[classifier retry] batch starting ${batchOffset}: attempt ${attempt} (status ${err.status || '?'}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
      },
    }
  );

  const text = (resp.content[0] && resp.content[0].text) || '';
  const parsed = parseJsonArray(text);

  // Map by index — defend against the LLM reordering or skipping entries
  const byIdx = {};
  for (const entry of parsed) {
    if (entry && typeof entry === 'object' && Number.isInteger(entry.i) && typeof entry.signal === 'string') {
      byIdx[entry.i] = { signal: entry.signal.trim().toUpperCase(), reason: (entry.reason || 'LLM classification').slice(0, 200) };
    }
  }
  // Build results in batch order; missing → null (orchestrator falls back to regex)
  return batch.map((_, i) => byIdx[i] || null);
}

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

/* ---------- Web verification (v3.2) ---------- */
// For each top accepted candidate, fire ONE focused Apify Google query of the
// form `"<Name>" <skill> (workshop OR training OR course OR bootcamp) India`.
// 2+ hits → +5 score boost (actively teaches). 0 hits → -3 demote (unverifiable).
// 1 hit → no change. Adds verifyNote to the candidate so RJP sees the reasoning
// in the Excel + UI.
//
// Cost: ~20 queries × $0.0035 = $0.07/brief. Latency: ~30s with concurrency=3.
// Disable with DISABLE_WEB_VERIFY=1. Skip in MOCK_APIFY mode.
const WEB_VERIFY_TOP_N        = parseInt(process.env.WEB_VERIFY_TOP_N || '20', 10);
const WEB_VERIFY_CONCURRENCY  = parseInt(process.env.WEB_VERIFY_CONCURRENCY || '3', 10);
const WEB_VERIFY_BOOST        = parseInt(process.env.WEB_VERIFY_BOOST || '5', 10);
const WEB_VERIFY_DEMOTE       = parseInt(process.env.WEB_VERIFY_DEMOTE || '3', 10);

function extractCandidateName(title) {
  if (!title) return '';
  // Title typically formed as "<Name> — <bio>" by upstream code, or
  // "<Name> at <Co>" / "<Name> - <bio>" from Google. Take first segment.
  const head = title.split(/[—–]|\s-\s|\bat\s/i)[0].trim();
  // Strip parens (often years/credentials), drop trailing punctuation
  return head.replace(/\s*\(.*?\)\s*/g, ' ').replace(/[.,;:|]+$/, '').trim().slice(0, 80);
}

async function webVerifyCandidates(client, accepted, bp, briefId) {
  const useVerify = process.env.MOCK_APIFY !== '1'
    && process.env.DISABLE_WEB_VERIFY !== '1'
    && accepted.length > 0;
  if (!useVerify) return accepted;

  const top = accepted.slice(0, WEB_VERIFY_TOP_N);
  const targets = top.map((c, idx) => {
    const name = extractCandidateName(c.title);
    if (!name || name.length < 4) return null; // skip if no usable name
    const kw = (c.keyword || bp.keywords[0] || '').slice(0, 50);
    const query = `"${name}" ${kw} (workshop OR training OR course OR bootcamp OR cohort) India`;
    return { idx, c, name, query };
  }).filter(Boolean);

  if (targets.length === 0) return accepted;
  if (briefId) logAndSave(briefId, `[verify] web-verifying ${targets.length}/${top.length} top accepted (concurrency ${WEB_VERIFY_CONCURRENCY})`);

  // Concurrent worker pool
  const verdicts = new Array(targets.length);
  let nextIdx = 0;
  const worker = async () => {
    while (true) {
      const i = nextIdx++;
      if (i >= targets.length) return;
      const t = targets[i];
      try {
        const items = await runGoogleQuery(
          client,
          { query: t.query, keyword: t.c.keyword, source: 'web-verify', tier: 'verify' },
          null  // suppress per-query log noise from the verify tier
        );
        verdicts[i] = items.length;
      } catch (_) {
        verdicts[i] = -1; // error → treat as no verification
      }
    }
  };
  await Promise.all(Array.from({ length: WEB_VERIFY_CONCURRENCY }, worker));

  // Apply adjustments
  const idxToVerdict = new Map();
  for (let i = 0; i < targets.length; i++) idxToVerdict.set(targets[i].idx, verdicts[i]);

  let boosted = 0, demoted = 0;
  const adjusted = accepted.map((c, idx) => {
    const v = idxToVerdict.get(idx);
    if (v == null) return c; // not in top N
    let delta = 0;
    let note = '';
    if (v >= 2) { delta = WEB_VERIFY_BOOST; note = `Web-verified: ${v} hit(s) for name + training keywords`; boosted++; }
    else if (v === 0) { delta = -WEB_VERIFY_DEMOTE; note = 'No web verification — name + training keywords returned 0 results'; demoted++; }
    else if (v === 1) { note = '1 weak verification hit'; }
    else { note = 'Verification query errored — score unchanged'; }
    return { ...c, score: Math.max(0, c.score + delta), webVerified: v, verifyNote: note };
  });

  if (briefId) logAndSave(briefId, `[verify] +${boosted} boosted, -${demoted} demoted, ${targets.length - boosted - demoted} unchanged`);
  return adjusted.sort((a, b) => b.score - a.score);
}

/* ---------- Pre-flight quality check (v3.2) ---------- */
// Before marking a brief complete, sample 3 candidates (rank 1, middle, last)
// and ask Haiku "would RJP defensibly send these to a client?" If <2/3 pass,
// log a warning that gets attached to the brief — RJP sees this on the detail
// view as part of the lowYield reasoning. This is a cheap final guard against
// runs where the engine accepted candidates but their fit-quality is borderline.
//
// Cost: 1 Haiku call, ~1K tokens = ~$0.001 per brief. Latency: ~6-9s.
async function preflightQualityCheck(capped, brief, bp, briefId) {
  const useCheck = hasLlmClient()
    && process.env.DISABLE_PREFLIGHT_CHECK !== '1'
    && capped.length >= 3;
  if (!useCheck) return null;

  const client = getAnthropic();
  if (!client) return null;

  // Sample top, middle, bottom for representative coverage
  const idxs = [0, Math.floor(capped.length / 2), capped.length - 1];
  const samples = idxs.map(i => capped[i]);

  try {
    const sys = `You are a senior client-relations consultant at a B2B trainer placement firm in India. For each candidate, judge: would you defensibly send this candidate to the client based on the brief? Reply ONLY a JSON array of "yes"/"no" verdicts in input order. No prose.`;

    const user = `Brief: ${brief.title || '(untitled)'}
Domain: ${brief.domain || '?'}
Keywords: ${bp.keywords.join(', ')}
Mode: ${bp.searchMode}
Operator direction: ${(bp.steering || '').slice(0, 400) || '(none)'}

Candidates (rank 1, mid, last):
${samples.map((c, i) => `${i + 1}. ${(c.title || '').slice(0, 150)}
   Signal: ${c.signal} | Score: ${c.score}
   Why: ${(c.reason || '').slice(0, 120)}
   Web verify: ${(c.verifyNote || '(none)').slice(0, 100)}`).join('\n\n')}

Reply: ["yes"|"no", "yes"|"no", "yes"|"no"]`;

    const resp = await withRetry(
      () => client.messages.create({
        model: CLAUDE_FAST,
        max_tokens: 80,
        system: sys,
        messages: [{ role: 'user', content: user }],
      }),
      { maxAttempts: 2, baseDelayMs: 3000 }
    );

    const text = ((resp.content[0] && resp.content[0].text) || '').toLowerCase();
    const arr = parseJsonArray(text);
    let yesCount = 0;
    for (const v of arr) {
      if (typeof v === 'string' && v.includes('yes')) yesCount++;
    }
    const passed = yesCount >= Math.ceil(samples.length * 2 / 3);
    if (briefId) {
      logAndSave(briefId, `[preflight] sampled rank 1/mid/last: ${yesCount}/${samples.length} pass defensibility check ${passed ? '✓' : '⚠'}`, passed ? 'info' : 'warn');
    }
    return { passed, yesCount, sampled: samples.length };
  } catch (e) {
    if (briefId) logAndSave(briefId, `[preflight] check errored (${(e.message || '').slice(0, 80)}) — skipped`, 'warn');
    return null;
  }
}

/* ---------- Multi-pass Sonnet rerank (v3.2) ---------- */
// After scoring + web-verify produces a sorted list, send the top 30 to
// Sonnet 4.5 with full bios + the brief context for one final holistic
// re-ranking. Sonnet sees things the per-candidate scorer can't:
//   - "this Founder has actually written a popular Aurora book → bookable"
//   - "this PRACTITIONER's 'about' mentions 8 yrs leading internal training"
//   - "this TRAINER_EXPLICIT is for an unrelated tech, demote"
//   - applying steering ("prioritise OmniStudio") across the ranked set
// Cost: 1 Sonnet call, ~30K input + ~3K output tokens = ~$0.10/brief.
// Latency: ~10-15s. Disable with DISABLE_RERANK=1.
const RERANK_TOP_N = parseInt(process.env.RERANK_TOP_N || '30', 10);

async function multiPassRerank(accepted, brief, bp, briefId) {
  const useRerank = hasLlmClient()
    && process.env.DISABLE_RERANK !== '1'
    && accepted.length >= 5;
  if (!useRerank) return accepted;

  const client = getAnthropic();
  if (!client) return accepted;

  const top = accepted.slice(0, RERANK_TOP_N);
  const outputN = bp.advanced.qualityCap || QUALITY_CAP_DEFAULT;
  if (briefId) logAndSave(briefId, `[rerank] Sonnet holistic rerank: top ${top.length} → top ${outputN}`);

  try {
    const sys = `You are an experienced sourcer at a B2B trainer placement firm in India. You re-rank candidates for a client brief based on holistic fit — cross-checking the classifier signal, the bio text, the source tier, and the web-verification result. You catch what the per-candidate scorer misses (e.g. a PRACTITIONER whose bio actually mentions 8 yrs of internal training, or a TRAINER_EXPLICIT whose tech is adjacent-but-not-target).

Brief context:
  Title:           ${brief.title || '(untitled)'}
  Keywords:        ${bp.keywords.join(', ')}
  Mode:            ${bp.searchMode}
  Must include:    ${(bp.must || []).join(', ') || '(none)'}
  Should include:  ${(bp.should || []).join(', ') || '(none)'}
  Must NOT:        ${(bp.mustNot || []).join(', ') || '(none)'}
  Excluded firms:  ${bp.exclusions.slice(0, 10).join(', ')}
  Operator steering: ${((bp.steering || '').slice(0, 800)) || '(none)'}

Output: JSON array of EXACTLY the top ${outputN} candidates (or fewer if input is smaller), best first.
Format: [{"i": <input index>, "why": "<≤18 word holistic judgment>"}, ...]`;

    const candidatesText = top.map((c, i) => {
      return `Candidate ${i}:
  Title:        ${(c.title || '').slice(0, 200)}
  Signal:       ${c.signal} (raw score: ${c.score})
  Reason:       ${(c.reason || '').slice(0, 150)}
  Web verify:   ${c.verifyNote || '(not verified)'}
  Sources:      ${(c.sources || []).join(', ') || c.tier || '?'}
  Keyword:      ${c.keyword || '?'}
  Snippet:      ${(c.text || '').slice(0, 300)}`;
    }).join('\n\n');

    const user = `Re-rank these ${top.length} candidates and return the top ${outputN}:\n\n${candidatesText}\n\nReturn JSON array now.`;

    const resp = await withRetry(
      () => client.messages.create({
        model: CLAUDE_SMART,
        max_tokens: Math.min(4000, Math.max(800, outputN * 90)),
        system: sys,
        messages: [{ role: 'user', content: user }],
      }),
      {
        maxAttempts: 2,
        baseDelayMs: 5000,
        onRetry: (attempt, delay, err) => {
          if (briefId) logAndSave(briefId, `[rerank retry] attempt ${attempt} (status ${err.status || '?'}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );

    const text = (resp.content[0] && resp.content[0].text) || '';
    const ranked = parseJsonArray(text);

    const newTop = [];
    const usedIdx = new Set();
    for (const r of ranked) {
      if (!r || typeof r !== 'object') continue;
      if (!Number.isInteger(r.i) || r.i < 0 || r.i >= top.length || usedIdx.has(r.i)) continue;
      usedIdx.add(r.i);
      const c = top[r.i];
      newTop.push({ ...c, rerankReason: (r.why || '').slice(0, 200), rerankPosition: newTop.length + 1 });
      if (newTop.length >= outputN) break;
    }

    if (newTop.length === 0) {
      if (briefId) logAndSave(briefId, `[rerank] Sonnet returned no usable rankings — keeping score-based order`, 'warn');
      return accepted;
    }

    if (briefId) logAndSave(briefId, `[rerank] Sonnet returned ${newTop.length} ranked candidates (replaced score-based order for the top)`);
    return newTop;
  } catch (e) {
    if (briefId) logAndSave(briefId, `[rerank] failed (${(e.message || '').slice(0, 100)}) — keeping score-based order`, 'warn');
    return accepted;
  }
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
  // v3.2: multi-source bonus — appearance in N>1 distinct tiers is a confidence signal.
  // +2 per extra source, capped at +6 (max 4 sources counted).
  const ms = (p.multiSource && p.multiSource > 1) ? Math.min(p.multiSource - 1, 3) * 2 : 0;
  return Math.round((sigPts + bktPts + verPts + bkPts) * norm + ms);
}

/* ---------- Reason-traceable accept/reject ---------- */
function buildDecision(p, bp) {
  // v3.2: tag multi-source confirmations into the reason so RJP can see why
  // a candidate scored higher than its raw signal would suggest.
  const multiSrcSuffix = (p.multiSource && p.multiSource > 1)
    ? ` · cross-confirmed in ${p.multiSource} sources (${(p.sources || []).join(', ')})`
    : '';
  if (REJECTED_SIGNALS.has(p.signal)) {
    return {
      decision: 'reject',
      decision_reason: (p.reason || 'Rejected — no trainer evidence') + multiSrcSuffix,
      decision_url: p.url || '',
      decision_snippet: (p.text || '').slice(0, 240),
    };
  }
  return {
    decision: 'accept',
    decision_reason: (p.reason || 'Trainer evidence present') + multiSrcSuffix,
    decision_url: p.url || '',
    decision_snippet: (p.text || '').slice(0, 240),
  };
}

/* ---------- Excel ---------- */
async function buildXlsx(brief, accepted, rejected, bp, timings) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'RJP Sourcing Portal v3.2';
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
    { header: 'Web verify', key: 'wv', width: 32 },
    { header: 'Book', key: 'b', width: 6 },
    { header: 'Tier', key: 'tier', width: 8 },
    { header: 'Keyword', key: 'kw', width: 22 },
  ];
  ws.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  ws.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E40AF' } };

  accepted.forEach((c, i) => {
    // Compose "why" with: classifier reason | rerank reasoning (if Sonnet ranked it)
    const whyParts = [c.decision_reason];
    if (c.rerankReason) whyParts.push(`Sonnet: ${c.rerankReason}`);
    ws.addRow({
      rank: i + 1,
      score: c.score,
      title: c.title || '(no title)',
      url: c.url || '',
      signal: c.signal,
      why: whyParts.join(' || '),
      src: c.decision_url || c.url || '',
      snip: c.decision_snippet || '',
      bf: c.bucketFit,
      v: c.verify,
      wv: c.verifyNote || (c.webVerified == null ? '(not verified)' : ''),
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

    // ---- Classification (LLM batched, regex fallback) ----
    store.update(briefId, { status: 'scoring' });
    const stopScore = stageStart('Scoring + classification');
    const itemsWithHarvest = normalised.map(it => ({
      it,
      harvest: harvestMap[canonLinkedinUrl(it.url)] || null,
    }));
    const classifications = await classifySignalsBatched(itemsWithHarvest, bp, briefId);

    const scored = normalised.map((it, i) => {
      const harvest = itemsWithHarvest[i].harvest;
      const cls = classifications[i];
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

    const acceptedRaw = scored.filter(c => c.decision === 'accept').sort((a, b) => b.score - a.score);
    const rejected = scored.filter(c => c.decision === 'reject');

    // ---- Web verification of top accepted candidates ----
    const stopVerify = stageStart('Web verification');
    const verified = await webVerifyCandidates(apifyClient, acceptedRaw, bp, briefId);
    stopVerify();

    // ---- Multi-pass Sonnet rerank ----
    const stopRerank = stageStart('Sonnet rerank');
    const accepted = await multiPassRerank(verified, brief, bp, briefId);
    stopRerank();

    // Apply quality cap (rerank already trims; this is a defensive backstop)
    const capped = accepted.slice(0, bp.advanced.qualityCap);
    stopScore();

    logAndSave(briefId, `Scored ${scored.length}: ${accepted.length} accepted (capped to ${capped.length}), ${rejected.length} rejected`);

    // ---- Pre-flight quality check ----
    const stopPreflight = stageStart('Pre-flight quality check');
    const preflightResult = await preflightQualityCheck(capped, brief, bp, briefId);
    stopPreflight();

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

    // Persist the Excel bytes to Postgres so it survives dyno restarts.
    // Render's free-tier filesystem is ephemeral — without this, every brief
    // becomes un-downloadable as soon as the dyno spins down (15 min idle).
    try {
      const bytes = fs.readFileSync(file);
      await store.saveOutput(briefId, path.basename(file), bytes);
      logAndSave(briefId, `[output] persisted Excel to Postgres (${(bytes.length / 1024).toFixed(1)} KB) — survives dyno restart`);
    } catch (e) {
      logAndSave(briefId, `[output] failed to persist Excel to Postgres: ${e.message}. File still on local FS but won't survive a Render restart.`, 'warn');
    }

    // Low-yield detection: don't silently produce an empty/thin Excel. Frontend
    // shows a yellow banner with the lowYieldReason so RJP knows the run isn't
    // deceptively "successful". Threshold = 3 because below that, the brief is
    // almost certainly missing the right keywords or has too-strict exclusions.
    const lowYieldThreshold = parseInt(process.env.LOW_YIELD_THRESHOLD || '3', 10);
    const countLow = capped.length < lowYieldThreshold;
    const preflightFailed = preflightResult && preflightResult.passed === false;
    const lowYield = countLow || preflightFailed;
    let lowYieldReason = null;
    if (capped.length === 0) {
      if (normalised.length === 0) {
        lowYieldReason = 'No candidates discovered from any source — Apify may have returned no results, or every tier failed (check log for retry warnings). Try broader keywords or check the Apify token.';
      } else {
        lowYieldReason = `Discovered ${normalised.length} candidates but ALL were filtered out. Most common cause: too-strict must-NOT terms, or the geo gate rejecting non-LinkedIn-enriched profiles. Review the Rejected (audit) tab.`;
      }
    } else if (countLow) {
      lowYieldReason = `Only ${capped.length} candidates surfaced (target ≥${lowYieldThreshold}). Consider broadening keywords, relaxing must-NOT, or switching to Niche mode if the tech is rare.`;
    } else if (preflightFailed) {
      lowYieldReason = `Pre-flight defensibility check: only ${preflightResult.yesCount}/${preflightResult.sampled} sampled candidates passed the "would you send this to the client" gate. Consider re-running with sharper keywords or stricter must-include terms.`;
    }
    if (lowYield) logAndSave(briefId, `Low-yield run: ${lowYieldReason}`, 'warn');

    store.update(briefId, {
      status: 'complete',
      outputFile: path.basename(file),
      counts: { discovered: normalised.length, accepted: capped.length, rejected: rejected.length, scored: scored.length },
      completedAt: new Date().toISOString(),
      timings,
      totalElapsed,
      acceptedSample: capped.slice(0, 10),
      rejectedSample: rejected.slice(0, 10),
      lowYield,
      lowYieldReason,
      preflight: preflightResult,
    });
    logAndSave(briefId, `Pipeline complete — ${capped.length} candidates, ${(stat.size / 1024).toFixed(1)} KB, ${totalElapsed}s total${lowYield ? ' [LOW YIELD]' : ''}`);
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

/* ---------- Watchdog: catch briefs stuck in non-terminal state ---------- */
// A brief is "stuck" if its status is one of {queued, discovery, scoring,
// packaging, preview} AND its most-recent log entry is older than the stuck
// threshold. Threshold default 25 min — comfortably above the worst-case
// niche TAT target of 12 min plus the LinkedIn enrichment 240s waitSecs and
// the 3-attempt retry budget (~5 min worst case per failed call).
//
// When the watchdog fires, the brief gets marked `failed` with an explanatory
// error AND a `retryable: true` flag so the frontend Retry button is offered.
// This is the safety net for the "back-end should never silently hang" rule —
// if the Node process restarted mid-pipeline, or if a runaway external call
// never returned, the brief no longer sits in 'discovery' indefinitely.
const STUCK_TIMEOUT_MS = parseInt(process.env.BRIEF_STUCK_TIMEOUT_MS || (25 * 60 * 1000), 10);
const WATCHDOG_INTERVAL_MS = parseInt(process.env.WATCHDOG_INTERVAL_MS || 60000, 10);
const NON_TERMINAL_STATUSES = new Set(['queued', 'discovery', 'scoring', 'packaging', 'preview']);

function startWatchdog() {
  setInterval(() => {
    const now = Date.now();
    const stuck = [];
    for (const b of store.list()) {
      if (!NON_TERMINAL_STATUSES.has(b.status)) continue;
      const lastLog = (b.log || []).slice(-1)[0];
      const lastTs = lastLog && lastLog.ts ? new Date(lastLog.ts).getTime() : new Date(b.createdAt || now).getTime();
      const stuckFor = now - lastTs;
      if (stuckFor > STUCK_TIMEOUT_MS) stuck.push({ b, mins: Math.round(stuckFor / 60000) });
    }
    for (const { b, mins } of stuck) {
      const reason = `Watchdog auto-failed: brief stuck in '${b.status}' for ${mins} min with no log activity. Likely a process restart or a hung external call. Click Retry to re-run.`;
      store.update(b.id, {
        status: 'failed',
        error: reason,
        retryable: true,
        log: (b.log || []).concat([{ ts: new Date().toISOString(), msg: reason, kind: 'err' }]),
      });
      console.warn(`[watchdog] auto-failed ${b.id} after ${mins}min in ${b.status}`);
    }
  }, WATCHDOG_INTERVAL_MS);
  console.log(`[watchdog] started — checking every ${WATCHDOG_INTERVAL_MS / 1000}s, stuck threshold ${STUCK_TIMEOUT_MS / 60000} min.`);
}

module.exports = { runPipeline, runWithFeedback, startWatchdog, OUTPUT_DIR, DEFAULT_BIGFIRM_EXCLUSIONS };
