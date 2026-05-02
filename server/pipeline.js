/**
 * RJP Sourcing Pipeline — v3.11.0
 *
 * v3.11.0 (2026-05-02 evening): Adds Phase 1.2 (L1.2) live-web named-trainer
 * probe via Perplexity Sonar Pro. Sits between Phase 1 (Claude knowledge prior)
 * and Phase 2 (Apify Google source-mining). Same ask as Phase 1 — named Indian
 * trainers with LinkedIn + evidence — but grounded in live web search, so it
 * picks up trainers active after the Claude cutoff, recent course launches,
 * fresh meetup organisers, and platforms Claude indexes weakly. ~$0.015 per
 * keyword (Sonar Pro). No-op while PERPLEXITY_API_KEY is unset — pipeline log
 * shows "[Phase 1.2] skipped" once per brief and continues to Phase 2.
 *
 * v3.10.1 (2026-05-02): Closes the v3.9 learning loop. multiPassRerank now
 * also reads accumulated `candidate_scores` from PRIOR briefs whose keywords
 * or domain overlap the active brief, formatted as OPERATOR-VERDICT PRIORS
 * in the Sonnet rerank system prompt — the team's actual booking decisions
 * become a self-reinforcing signal alongside `seed-client-gradings.json`.
 * Excludes the active brief itself (avoid self-priming feedback). Capped at
 * 30 rows in prompt to keep size sane. Empty when no prior verdicts match.
 *
 * v3.10.0 (2026-05-02): Chat-style input + AskUserQuestion follow-up flow
 * (server-side: new POST /api/briefs/ask-questions endpoint in server.js
 * generating up to 15 multi-choice clarifying questions via Haiku; pipeline
 * itself unchanged — operator's answers get appended to the brief's steering
 * field as a CLARIFYING ANSWERS block, so the existing prompts in L1/L2/L3,
 * classifier, and Sonnet rerank pick them up automatically without schema
 * change). Frontend wizard is demoted behind an "advanced" link; default
 * "+ New brief" goes to the chat view (Perplexity-style: type description →
 * Haiku parse → multi-choice follow-ups → confirm → submit).
 *
 * v3.9.0 (2026-05-02): Three feature-shipments in one go.
 *   1. Iteration summary (Vijay's "send him iteration history & best matches"
 *      ask): runPipeline now records `iterationSummary[]` on the brief — one
 *      entry per run with rev number, top-5 names, who newly entered, who
 *      dropped vs the prior run, candidate count, and the feedback text that
 *      drove the change. Rendered as a collapsible card at the top of the
 *      detail view so Vijay can see the iteration trail at a glance.
 *   2. Explicit 3-phase pipeline naming (Ramesh's framing): pipeline log
 *      labels updated to PHASE 1 (top-of-stack named trainers via Claude
 *      knowledge), PHASE 2 (keyword-combination expansion via Google), PHASE
 *      3a (adjacent technologies), 3b (institutes), and the new 3c — founders
 *      / principals at small Indian firms specialising in the target tech
 *      (`claudeFoundersAtSmallFirms`). Phase 3c fires for niche briefs and
 *      thin-result reruns. These small-firm founders are bookable for
 *      high-touch corporate training even when they don't self-identify as
 *      "trainers" — Ramesh's exact "alternative people" pool from the SOP.
 *   3. Candidate-scoring schema lands (server table only this ship — UI
 *      buttons in v3.9 frontend). Backend stores Selected/Hold/Rejected per
 *      candidate (matches Saranya's grading taxonomy from v3.8.0). Future
 *      reranks will read these as additional priors alongside the seed file.
 *
 * v3.8.0 (2026-05-02): Client-side grading seed file
 * (`server/seed-client-gradings.json`) loaded at module init. The reviewer at
 * RJP's end client (Saranya) graded 8 candidates from a Perplexity-generated
 * Splunk Observability list as Selected (3) / Hold-for-call (4) / Rejected (1)
 * with verbatim reasons. Distilled into rules + few-shot examples and injected
 * into `multiPassRerank`'s system prompt as CLIENT-SIDE GRADING PRIORS when
 * the active brief's keywords overlap a grading. Sonnet biases toward the
 * same selectivity standard the end-client applies — reduces "looked good in
 * Excel, client said no" mismatch. DOMAIN_TUNING also gains a `splunk` entry
 * reflecting Saranya's strict "BOTH keywords + explicit Trainer role" rule.
 * Schema is generic so future client gradings (other domains/reviewers) just
 * append to the array — no code change needed.
 *
 * v3.7 (2026-05-02): Two robustness fixes from the Netbrain + Splunk-1
 * Form-1 feedback iterations:
 *   1. Clarify endpoint (v3.6.1) now also catches single-word / generic
 *      keywords (e.g., "splunk", "trainer", "freelance") AND over-long
 *      must/should/mustNot items (>5 words). Both passed v3.6's prose
 *      detector but produced useless searches. Surfaces a draft with
 *      compound role-anchored keyword suggestions.
 *   2. Pipeline post-classifier safety: signal/reason consistency check.
 *      When Haiku returns a positive signal (TRAINER_EXPLICIT etc.) AND a
 *      reason text containing negation phrases ("no training", "rejected
 *      — out of domain", "non-X focus"), the candidate is demoted to NOISE.
 *      Defends against the LLM contradicting itself under strict steering.
 *
 * v3.6 (2026-05-02): Conversational pre-submit clarification. New
 * `clarifyInput` helper + POST /api/briefs/clarify endpoint return a
 * verdict (clear / needs_clarification / unsalvageable) with diagnostic
 * issues, a suggested clean draft of the keywords, and clarifying questions.
 * Wizard step 4 calls it on entry and shows the verdict inline. Operator
 * can use the suggestion (`confirmedClean: true`, skips pipeline cleaner)
 * or override with the original input (pipeline cleaner remains as safety
 * net). Unsalvageable verdict blocks submission with a clear reason +
 * working examples.
 *
 * v3.5 (2026-05-02): Two robustness fixes from the Splunk-brief audit:
 *   1. Smart keyword cleanup — handles paste from WhatsApp/email/LLM output/
 *      Word docs/course outlines. Mechanical strip + Haiku-driven extraction
 *      when prose is detected. Original messy input preserved as
 *      `originalMessyKeywords` for audit.
 *   2. Post-classifier regex safety net — guarantees BIG_FIRM/NON_INDIA/
 *      MUSTNOT_HIT rules are honored even when the LLM classifier hallucinates
 *      past them. Regex is authoritative on hard-exclude rules.
 *
 * v3.4 (2026-05-02): Excel output now matches the 14-column Word-doc spec
 * (Name, Role, Company, Domain Skill, Location, Email Official + Personal,
 * Mobile, LinkedIn, Website, No. of Trainings, Domain Trainings, Activity
 * Score H/M/L, Remarks). harvestapi data (location, headline, experience)
 * carried through to the candidate object. Engineering rubric moved to a
 * second sheet "Engineering details". Email/Mobile/Trainings counts marked
 * as interview-time fields per the SOP's Step 5.
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

// Perplexity Sonar Pro (Phase 1.2 / L1.2). Required-by-feature, optional-at-boot:
// hasPerplexity() returns false until PERPLEXITY_API_KEY is set, at which point
// the Phase 1.2 block in runPipeline begins firing. No spend, no behavior change
// while disabled.
const { perplexityChat, hasPerplexity } = require('./perplexity-client');

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
  // v3.3: source-toggle defaults. New v3.3 sources are ON by default so
  // existing briefs (created before the new keys existed) automatically benefit
  // from broader pool coverage. Operators can disable individual sources
  // per-brief via the Step 3 source-toggles UI.
  const DEFAULT_SOURCES = {
    linkedin: true,
    urbanpro: true,
    youtube: true,
    udemy: true,
    blogs: true,
    // v3.3 additions
    indianPlatforms: true,        // Edureka, Simplilearn, GreatLearning, UpGrad, AnalyticsVidhya, Whizlabs, KodeKloud
    authorityDirectories: true,   // Microsoft MVP, Google GDE, AWS Hero, Salesforce Trailblazer, CNCF Ambassador
    coursePlatforms: true,        // Coursera, Pluralsight, LinkedIn Learning, O'Reilly
    meetup: true,                 // meetup.com — community organizers (high training-delivery signal)
    eventbrite: true,             // workshop hosts in India
    github: true,                 // educational repo authors (best for dev/tech roles)
  };
  const advanced = Object.assign({
    queryDepth: searchMode === 'niche' ? MAX_QUERIES_NICHE : MAX_QUERIES_STD,
    weights: { signal: 40, bucket: 30, verify: 15, book: 15 },
    qualityCap: QUALITY_CAP_DEFAULT,
  }, brief.advanced || {});
  // Merge sources separately so a brief that sets only e.g. `{linkedin:true}`
  // doesn't wipe defaults for the v3.3 keys it doesn't know about.
  advanced.sources = Object.assign({}, DEFAULT_SOURCES, advanced.sources || {});
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
  'splunk':           'Client-side reviewer (Saranya, 2026-05-02) selects ONLY profiles with BOTH "Splunk" AND "Observability" present in the bio AND an explicit Trainer role (Freelance, Corporate, Independent, or Instructor). Holds Consultants — they need a verification call before sending. Rejects Architects (regardless of depth), profiles missing both keywords, and out-of-India locations. Currently-employed-at-Splunk practitioners are HOLD not SELECT (not freelance). Match this strictness in the rerank.',
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

/* ---------- Client-side grading seed (v3.8.0) ---------- */
// Load `seed-client-gradings.json` at module init. Each grading is a list of
// candidates with their reviewer-assigned verdicts (selected/hold/rejected) and
// verbatim reasons. multiPassRerank looks up gradings whose keywords or domain
// overlap the active brief's, then injects them into Sonnet's system prompt as
// few-shot examples — the goal is to bias the rerank toward the same standard
// the end-client reviewer actually applies.
//
// Graceful degradation: missing or malformed seed file = empty list = the
// rerank prompt is unchanged (legacy behaviour). Logged once at boot so the
// operator knows whether priors are active.
const SEED_GRADINGS_FILE = path.join(__dirname, 'seed-client-gradings.json');
let _CLIENT_GRADINGS = [];
try {
  if (fs.existsSync(SEED_GRADINGS_FILE)) {
    const parsed = JSON.parse(fs.readFileSync(SEED_GRADINGS_FILE, 'utf8'));
    _CLIENT_GRADINGS = Array.isArray(parsed.gradings) ? parsed.gradings : [];
    if (_CLIENT_GRADINGS.length) {
      console.log(`[pipeline v3.8] loaded ${_CLIENT_GRADINGS.length} client-side grading(s) from ${path.basename(SEED_GRADINGS_FILE)} — will inject into rerank when keywords overlap`);
    }
  }
} catch (e) {
  console.warn('[pipeline v3.8] seed-client-gradings.json malformed; gradings disabled:', e.message);
}

function findRelevantGradings(bp) {
  if (!_CLIENT_GRADINGS.length) return [];
  const briefKws = (bp.keywords || []).map(k => String(k).toLowerCase());
  const briefDom = String(bp.domain || '').toLowerCase();
  const matched = [];
  for (const g of _CLIENT_GRADINGS) {
    const gKws = (g.keywords || []).map(k => String(k).toLowerCase());
    const gDom = String(g.domain || '').toLowerCase();
    // Keyword overlap: substring match either direction (so 'splunk' grading matches
    // both 'Splunk Trainer' and 'Splunk Cloud Observability' briefs).
    const kwHit = gKws.some(gk => briefKws.some(bk => bk.includes(gk) || gk.includes(bk)));
    const domHit = !!(gDom && briefDom && (briefDom.includes(gDom) || gDom.includes(briefDom)));
    if (kwHit || domHit) matched.push(g);
  }
  return matched;
}

/* v3.10.1 — Operator-verdict priors. Format saved Selected/Hold/Rejected
   verdicts from PRIOR similar briefs as a few-shot block for the Sonnet
   rerank. Caller filters by keyword/domain overlap (store.getOperatorVerdictsForBriefContext).
   Returns '' when no matching verdicts exist. */
function formatOperatorVerdictsForPrompt(verdicts) {
  if (!Array.isArray(verdicts) || verdicts.length === 0) return '';
  const counts = { selected: 0, hold: 0, rejected: 0 };
  for (const v of verdicts) counts[v.score] = (counts[v.score] || 0) + 1;
  const byVerdict = { selected: [], hold: [], rejected: [] };
  for (const v of verdicts) {
    if (byVerdict[v.score]) byVerdict[v.score].push(v);
  }
  const renderRow = (v) => {
    const ctx = v.briefTitle ? ` (from prior brief "${String(v.briefTitle).slice(0, 40)}")` : '';
    const noteStr = v.note ? ` — note: ${String(v.note).slice(0, 80)}` : '';
    return `      • ${v.candidateName || '(unnamed)'}${ctx}${noteStr}`;
  };
  const examples = ['selected', 'hold', 'rejected'].map(s => {
    const rows = byVerdict[s].slice(0, 5);
    if (!rows.length) return null;
    return `    ${s.toUpperCase()} (${counts[s] || 0} total in this rerank context):\n` + rows.map(renderRow).join('\n');
  }).filter(Boolean).join('\n');
  return `\n\nOPERATOR-VERDICT PRIORS (Selected/Hold/Rejected calls already made by THIS team on PRIOR similar briefs — these are the team's own booking decisions, treat as authoritative learning signal):\n${examples}\n\nWhen ranking, prefer profiles matching the SELECTED pattern; downgrade HOLD-style profiles; treat anything matching REJECTED as a hard demote. These priors compose with the CLIENT-SIDE GRADING PRIORS above — when both fire, they should agree on direction.`;
}

function formatGradingsForPrompt(gradings) {
  if (!gradings.length) return '';
  const blocks = gradings.map(g => {
    const counts = { selected: 0, hold: 0, rejected: 0 };
    for (const c of (g.candidates || [])) counts[c.verdict] = (counts[c.verdict] || 0) + 1;
    const rules = (g.rules_distilled || []).map((r, i) => `  ${i + 1}. ${r}`).join('\n');
    // Show up to 2 examples per verdict tier — enough for pattern, not so much
    // that the prompt bloats. Verbatim reason is the key signal.
    const byVerdict = { selected: [], hold: [], rejected: [] };
    for (const c of (g.candidates || [])) {
      if (byVerdict[c.verdict]) byVerdict[c.verdict].push(c);
    }
    const renderRow = (c) => `      • ${c.name} (${c.place || '?'}) — "${String(c.headline || '').slice(0, 110)}" → reason: ${c.reason}`;
    const examples = ['selected', 'hold', 'rejected'].map(v => {
      const rows = byVerdict[v].slice(0, 2);
      if (!rows.length) return null;
      return `    ${v.toUpperCase()} (${counts[v] || 0} total in this grading):\n` + rows.map(renderRow).join('\n');
    }).filter(Boolean).join('\n');
    return `Reviewer: ${g.reviewer}
Brief: ${g.source_brief}
Date: ${g.received_at}
Distribution: ${counts.selected || 0} selected / ${counts.hold || 0} hold / ${counts.rejected || 0} rejected
Distilled rules:
${rules}
Examples:
${examples}`;
  }).join('\n\n---\n\n');
  return `\n\nCLIENT-SIDE GRADING PRIORS (apply this standard — these are how the END CLIENT actually decides what to Select vs Hold vs Reject):\n${blocks}\n\nWhen ranking, prefer profiles matching the SELECTED pattern, downgrade those matching HOLD, treat anything matching REJECTED as a hard demote. Reviewer's verbatim reasons above are authoritative.`;
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
      // v3.3: refined to target specific high-signal blogging platforms
      // instead of generic "anything not LinkedIn". Hashnode is Indian-founded
      // and dev-heavy; Dev.to and Medium have strong Indian tech-writer pools;
      // Substack catches paid-newsletter authors who often run cohorts.
      perKw.push({
        query: `${kwClean} trainer India site:hashnode.com OR site:dev.to OR site:medium.com OR site:substack.com`,
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
    // v3.3 — Indian training platforms. These ARE corporate trainers (it's
    // their literal job). Combined into one OR-query so we don't burn 7
    // separate queries per keyword when the cap is 12.
    if (sites.indianPlatforms) {
      perKw.push({
        query: `${kwClean} instructor India site:edureka.co OR site:simplilearn.com OR site:greatlearning.com OR site:upgrad.com OR site:analyticsvidhya.com OR site:whizlabs.com OR site:kodekloud.com`,
        keyword: kw, variant: 'indian-platform', source: 'indian-platforms', tier: 'L1.3',
      });
    }
    // v3.3 — Authority directories. Vendor-curated MVP / GDE / Hero lists.
    // Highest signal-to-noise of any source — these people have been
    // explicitly vetted by Microsoft / Google / AWS / Salesforce / CNCF.
    if (sites.authorityDirectories) {
      perKw.push({
        query: `${kwClean} India site:mvp.microsoft.com OR site:developers.google.com/community OR site:aws.amazon.com/heroes OR site:trailblazer.me OR site:cncf.io/people`,
        keyword: kw, variant: 'authority', source: 'authority-directory', tier: 'L1.3',
      });
    }
    // v3.3 — Course platforms (instructor pages on Coursera, Pluralsight,
    // LinkedIn Learning, O'Reilly). Good for vetted senior-level trainers,
    // strong India representation on Pluralsight especially.
    if (sites.coursePlatforms) {
      perKw.push({
        query: `${kwClean} instructor India site:coursera.org OR site:pluralsight.com OR site:linkedin.com/learning OR site:oreilly.com`,
        keyword: kw, variant: 'course-platform', source: 'course-platform', tier: 'L1.3',
      });
    }
    // v3.3 — Meetup organizers. Indian meetup organizers have to teach for
    // free; that filters for genuine pedagogical skill. L4 tier because
    // mixing speakers and trainers — classifier handles the distinction.
    if (sites.meetup) {
      perKw.push({
        query: `${kwClean} meetup organizer India site:meetup.com`,
        keyword: kw, variant: 'meetup', source: 'meetup', tier: 'L4',
      });
    }
    // v3.3 — Eventbrite workshop hosts. Pay-per-event hosts are usually
    // bookable trainers with their own brand.
    if (sites.eventbrite) {
      perKw.push({
        query: `${kwClean} workshop India site:eventbrite.com`,
        keyword: kw, variant: 'eventbrite', source: 'eventbrite', tier: 'L4',
      });
    }
    // v3.3 — GitHub educational repos. The author of a repo with topic
    // 'tutorial' / 'course' / 'bootcamp' is by definition teaching. L4
    // tier; only useful for tech/dev domains (not SAP, not HR).
    if (sites.github) {
      perKw.push({
        query: `${kwClean} tutorial OR course OR bootcamp India site:github.com`,
        keyword: kw, variant: 'github-edu', source: 'github', tier: 'L4',
      });
    }
    // v3.3 — Domain-specific authority queries. Fire only when brief.domain
    // matches; these are highly-targeted but only useful for those domains.
    const dom = (brief.domain || '').toLowerCase();
    if (dom.includes('salesforce')) {
      perKw.push({
        query: `${kwClean} Ranger India site:trailblazer.me`,
        keyword: kw, variant: 'salesforce-ranger', source: 'trailblazer', tier: 'L1.3',
      });
    }
    if (dom.includes('data') || dom.includes('ml ') || dom.includes(' ml') || dom.includes('ai') || dom.includes('machine') || dom.includes('science')) {
      perKw.push({
        query: `${kwClean} India site:huggingface.co OR site:kaggle.com`,
        keyword: kw, variant: 'ml-community', source: 'ml-community', tier: 'L1.3',
      });
    }
    if (dom.includes('sap')) {
      perKw.push({
        query: `${kwClean} India site:community.sap.com OR site:sap-press.com`,
        keyword: kw, variant: 'sap-community', source: 'sap-community', tier: 'L1.3',
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

/* L1.2 — Perplexity Sonar Pro live-web probe (v3.11.0).
   Same named-trainer ask as L1.1, but grounded in live web search rather than
   Claude's training-data prior. Catches trainers who became active after the
   Claude knowledge cutoff, recent course launches, fresh meetup organisers,
   and trainers whose primary footprint is on platforms Claude indexes weakly
   (UrbanPro listings, Substack, recent YouTube uploads). Each result carries
   a citation URL from Sonar's web search, which we keep in _perplexitySource
   so downstream classifier/rerank can use it as second-source evidence.
   ~$0.015 per keyword (Sonar Pro). No-op when PERPLEXITY_API_KEY is unset. */
async function perplexityKnowledgeCall(briefId, kw, bp) {
  if (!hasPerplexity()) return [];
  try {
    const sys = `You are a sourcing assistant for a B2B trainer placement firm in India. Search the live web and return named freelance corporate trainers, independent instructors, consultants, SMEs, or architects in India who currently deliver training in the target technology. Do NOT return conference speakers without training-delivery evidence. Do NOT return employees of: ${bp.exclusions.slice(0, 8).join(', ')}.${domainHint(bp.domain)}${steeringHint(bp.steering)}`;
    const user = `Technology / skill: ${kw}\nLocation: India only.\nMode: ${bp.searchMode === 'niche' ? 'niche (rare tech, prefer founders/principals of small Indian firms)' : 'standard'}\nReturn 5-10 currently-active named candidates verified from your web search. For each: name, LinkedIn URL (must be a real URL you found, not guessed), 1-line evidence of training-delivery (course, workshop, repeated training engagements), and the source URL where you verified them. Output ONLY a JSON array: [{"name":"","linkedin":"","evidence":"","source":""}]. No prose before or after the JSON.`;
    const resp = await withRetry(
      () => perplexityChat({ system: sys, user, maxTokens: 1500 }),
      {
        maxAttempts: 2,
        baseDelayMs: 4000,
        onRetry: (attempt, delay, err) => {
          if (briefId) logAndSave(briefId, `[L1.2 retry] Perplexity attempt ${attempt} for "${kw}" failed (status ${err.status || '?'}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );
    return parseJsonArray(resp.text).map(p => ({
      url: p.linkedin || p.source || '',
      metadata: { title: `${p.name} — Perplexity web L1.2` },
      markdown: `${p.name}. ${p.evidence || ''}. India.${p.source ? ' Source: ' + p.source : ''}`,
      _q: { keyword: kw, source: 'perplexity_web', tier: 'L1.2' },
      _claudeNote: p.evidence || '',
      _perplexitySource: p.source || '',
    })).filter(p => p.url || p.metadata.title);
  } catch (e) {
    if (briefId) logAndSave(briefId, `[L1.2] Perplexity gave up for "${kw}" after retries: ${(e.message || '').slice(0, 100)}. Continuing to Phase 2.`, 'warn');
    console.warn('[perplexity L1.2]', e.message);
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

/* ---------- Phase 3c: Founders / principals at small Indian firms (v3.9.0) ----------
   Ramesh's Phase 3 from the call: when straight trainer searches go thin,
   look for founders/principals/owners of SMALL Indian firms (≤50 employees)
   specialising in the target tech. They deliver training as part of their
   billable consulting practice, even when they don't self-label as "trainer".
   Fires when L1+L2+L3 still produced fewer than ~12 unique candidates, OR
   when the brief is in niche mode. Costs ~$0.005 per keyword (Haiku).        */
async function claudeFoundersAtSmallFirms(briefId, kw, bp) {
  const client = getAnthropic();
  if (!client) return [];
  try {
    const sys = `You list founders/principals/owners/CXOs of SMALL Indian firms (preferably 1-50 employees, NOT the big SI firms — exclude ${bp.exclusions.slice(0, 8).join(', ')}) that specialise in the target technology. They deliver corporate training as part of their consulting/services practice even when they don't self-label as "trainer". Pool: ex-FTEs of the principal vendor (Splunk, AWS, Salesforce etc.) who founded boutique consultancies. Concise, India-focused, verifiable.${domainHint(bp.domain)}${steeringHint(bp.steering)}`;
    const user = `Technology / skill: ${kw}\nList up to 6 founders/principals at small Indian firms specialising in this. Output JSON: [{"name":"","firm":"","role":"Founder/Principal/Owner/CXO","city":"","linkedin":""}].`;
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
          if (briefId) logAndSave(briefId, `[Phase 3c retry] Founders attempt ${attempt} for "${kw}" failed (status ${err.status || '?'}); retrying in ${Math.round(delay / 1000)}s`, 'warn');
        },
      }
    );
    const text = (resp.content[0] && resp.content[0].text) || '';
    return parseJsonArray(text).map(p => ({
      url: p.linkedin || '',
      metadata: { title: `${p.name} — ${p.role || 'Founder'} at ${p.firm || ''}` },
      markdown: `${p.name}. ${p.role || 'Founder'} at ${p.firm || ''}. ${p.city ? p.city + ', ' : ''}India. Small-firm ${(p.role || 'founder').toLowerCase()} — bookable for high-touch corporate training (Ramesh's Phase 3 alternative-people pool).`,
      _q: { keyword: kw, source: 'claude_founders', tier: 'L_FOUNDERS' },
      _claudeNote: `Founder/principal at ${p.firm || 'small firm'} — high-touch training-bookable per the SOP's Phase 3 framing`,
    })).filter(p => p.url || (p.metadata && p.metadata.title));
  } catch (e) {
    if (briefId) logAndSave(briefId, `[Phase 3c] Founders gave up for "${kw}" after retries: ${(e.message || '').slice(0, 100)}.`, 'warn');
    console.warn('[claude founders]', e.message);
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
// Default 2 to match the global CLI semaphore (anthropic-claude-cli.js MAX_CONCURRENT_CLI).
// Any higher and individual brief batches still queue at the global semaphore — extra
// per-brief concurrency just buys nothing and complicates reasoning about resource use.
const LLM_CLASSIFIER_CONCURRENCY = parseInt(process.env.LLM_CLASSIFIER_CONCURRENCY || '2', 10);
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

  // v3.10.1 — Operator-verdict priors. Pull Selected/Hold/Rejected calls
  // from prior similar briefs and format as a few-shot block. Done BEFORE
  // the main try so failures in this query don't tank the whole rerank.
  let operatorPriorBlock = '';
  try {
    const verdicts = await store.getOperatorVerdictsForBriefContext(briefId, bp.keywords, bp.domain);
    if (verdicts && verdicts.length) {
      operatorPriorBlock = formatOperatorVerdictsForPrompt(verdicts);
      if (briefId) logAndSave(briefId, `[rerank] loaded ${verdicts.length} operator-verdict prior(s) from prior similar briefs`);
    }
  } catch (e) {
    if (briefId) logAndSave(briefId, `[rerank] operator-verdict prior load failed (${(e.message || '').slice(0, 80)}) — continuing without`, 'warn');
  }

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
  Operator steering: ${((bp.steering || '').slice(0, 800)) || '(none)'}${formatGradingsForPrompt(findRelevantGradings(bp))}${operatorPriorBlock}

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

/* ---------- Spec-column helpers (v3.4) ----------
   The Word doc requirement (RJPInfotek_ManualProcess_Brief / IT Technical
   Trainer Identification) lists 14 named columns the searcher uses to
   schedule a call: Name, Role, Company, Domain Skill, Location, Email
   (Official + Personal), Mobile, LinkedIn URL, Website, No. of Trainings,
   Domain Trainings, Activity Score (H/M/L), Remarks.

   Email/Mobile/Trainings counts are by-spec interview-time fields (Step 5
   "Secondary Enrichment over an interview call") — left blank with the
   interview marker so the sourcer knows the column is intentional, not
   a missing scrape. Name/Role/Company are derived from the title +
   harvestapi headline + first experience entry. Activity Score H/M/L
   maps from the existing 0-100 composite. */

const INTERVIEW_PLACEHOLDER = '— (interview)';

function splitNameRoleCompany(c) {
  const harvest = c.harvest || {};
  const title = (c.title || '').trim();
  const headline = (harvest.headline || '').trim();
  const exp = (harvest.experience && harvest.experience[0]) || {};

  // Title is typically "<Name> - <Role/Headline>" (Google snippet shape) or
  // "<Name> | <Co>" or "<Name> at <Co>". Split on the first such separator.
  let name = title;
  let rest = '';
  const m = title.match(/^(.+?)\s+(?:[-—–|]|\bat\b)\s+(.+)$/);
  if (m) { name = m[1]; rest = m[2]; }
  // Strip trailing "| LinkedIn", "- LinkedIn", "· LinkedIn"
  name = name.replace(/\s*[|\-·]\s*LinkedIn\s*$/i, '').trim();
  rest = rest.replace(/\s*[|\-·]\s*LinkedIn\s*$/i, '').trim();

  // Company: prefer harvestapi experience[0], else extract from headline/title.
  // Order of fallbacks:
  //   1. "<role> at <Co>" / "<role> @ <Co>"  — most common LinkedIn shape
  //   2. "Founder/Principal/Owner/Director/CEO ... of <Co>" — when no experience
  //      entry exists (typical for self-employed founders)
  //   3. "Founder, <Co>" / "Owner, <Co>"  — comma form
  let company = exp.company || exp.companyName || '';
  if (!company) {
    const blob = (rest + ' ' + headline);
    const at = blob.match(/\b(?:at|@)\s+([A-Z][\w&.,'\- ]{2,50})/);
    if (at && !/linkedin/i.test(at[1])) company = at[1].trim();
    if (!company) {
      const founderOf = blob.match(/\b(?:founder|co-founder|principal|owner|director|ceo|cto|cfo|managing\s+partner|managing\s+director|chief\s+\w+)[\w&,'.\- ]{0,30}\s+of\s+([A-Z][\w&.,'\- ]{2,50})/i);
      if (founderOf) company = founderOf[1].trim();
    }
    if (!company) {
      const founderComma = blob.match(/\b(?:founder|co-founder|owner|principal|managing\s+partner)\s*,\s*([A-Z][\w&.,'\- ]{2,50})/i);
      if (founderComma) company = founderComma[1].trim();
    }
  }
  // Tidy trailing punctuation/possessive
  company = company.replace(/[.,;:|]+$/, '').trim();

  return {
    name: name.slice(0, 120) || '(no name)',
    role: (rest || headline).slice(0, 200),
    company: company.slice(0, 120),
  };
}

function roleFromSignal(signal) {
  return ({
    TRAINER_EXPLICIT:  'Corporate Trainer',
    FREELANCE_TRAINER: 'Freelancer',
    TRAINER_IMPLIED:   'Consultant',
    INSTITUTE:         'Institute',
    PRACTITIONER:      'Working Professional',
  })[signal] || '—';
}

function activityBand(score) {
  // Bands match the existing colour fills (≥85 high, 65-84 mid-high, 40-64
  // mid, <40 low). Spec asks for High/Medium/Low so collapse the top two.
  if (score >= 65) return 'High';
  if (score >= 40) return 'Medium';
  return 'Low';
}

function locationOf(c) {
  const h = c.harvest || {};
  if (h.location) return String(h.location).slice(0, 100);
  if (h.countryCode === 'IN') return 'India';
  // Fall back to scanning text/title for a known Indian city
  const blob = ((c.title || '') + ' ' + (c.text || '')).toLowerCase();
  for (const city of INDIAN_CITIES) {
    if (new RegExp('\\b' + city + '\\b', 'i').test(blob)) {
      return city.charAt(0).toUpperCase() + city.slice(1) + ', India';
    }
  }
  return '';
}

function extractWebsite(c) {
  const h = c.harvest || {};
  if (h.website) return String(h.website).slice(0, 120);
  if (h.contactInfo && Array.isArray(h.contactInfo.websites) && h.contactInfo.websites[0]) {
    return String(h.contactInfo.websites[0]).slice(0, 120);
  }
  // Last resort: first non-LinkedIn, non-asset URL in the bio/snippet.
  // Skip image/font extensions and known asset CDN hosts (urbanpro page icons,
  // LinkedIn media, Google fonts, CloudFront, googleusercontent, akamai). The
  // v3.4 first run surfaced `https://c.urbanpro.com/.../urbanpro_icon-...png`
  // as a "website" — that's a page-decoration favicon, not the trainer's site.
  const txt = (c.text || '') + ' ' + (h.about || '');
  const urlRe = /https?:\/\/(?!(?:www\.|[a-z]{2}\.)?linkedin\.com)[\w.-]+\.[a-z]{2,}[^\s<>"')]*/gi;
  const skipExt = /\.(?:png|jpe?g|gif|svg|ico|webp|bmp|css|js|woff2?|ttf|eot)(?:\?|#|$)/i;
  const skipHost = /^https?:\/\/(?:c\.urbanpro\.com|fonts\.(?:gstatic|googleapis)\.com|media-[\w-]+\.licdn\.com|[\w-]+\.cloudfront\.net|[\w-]+\.googleusercontent\.com|[\w-]+\.akamaized\.net)/i;
  const urls = txt.match(urlRe) || [];
  for (const u of urls) {
    if (skipExt.test(u)) continue;
    if (skipHost.test(u)) continue;
    return u.slice(0, 120);
  }
  return '';
}

function buildRemarks(c) {
  const parts = [];
  if (c.decision_reason) parts.push(c.decision_reason);
  if (c.rerankReason)    parts.push('Sonnet: ' + c.rerankReason);
  if (c.verifyNote)      parts.push('Web: ' + c.verifyNote);
  parts.push('Score ' + c.score + ' (' + (c.signal || '') + ')');
  return parts.join(' · ');
}

/* ---------- Excel ---------- */
async function buildXlsx(brief, accepted, rejected, bp, timings) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'RJP Sourcing Portal v3.4';
  wb.created = new Date();

  // -------- 1. Candidates — 14-column spec sheet (Word doc schema) --------
  const ws = wb.addWorksheet('Candidates');
  ws.columns = [
    { header: 'Rank',                       key: 'rank',     width: 6 },
    { header: 'Name',                       key: 'name',     width: 28 },
    { header: 'Role',                       key: 'role',     width: 22 },
    { header: 'Company',                    key: 'company',  width: 24 },
    { header: 'Domain Skill',               key: 'domain',   width: 22 },
    { header: 'Location',                   key: 'location', width: 22 },
    { header: 'Email (Official)',           key: 'email1',   width: 22 },
    { header: 'Email (Personal)',           key: 'email2',   width: 22 },
    { header: 'Mobile Number',              key: 'mobile',   width: 16 },
    { header: 'LinkedIn URL',               key: 'linkedin', width: 44 },
    { header: 'Website / Portfolio',        key: 'website',  width: 30 },
    { header: 'No. of Trainings Conducted', key: 'tcount',   width: 12 },
    { header: 'Relevant Domain Trainings',  key: 'tdomain',  width: 12 },
    { header: 'Activity Score',             key: 'activity', width: 12 },
    { header: 'Remarks',                    key: 'remarks',  width: 60 },
  ];
  ws.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  ws.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1E40AF' } };

  accepted.forEach((c, i) => {
    const split = splitNameRoleCompany(c);
    // Prefer the signal-derived role label over the headline tail — the spec
    // asks for one of {Freelancer, Corporate, Working Professional, ...}, not
    // a free-form bio fragment. Keep the bio fragment in Remarks.
    const role = roleFromSignal(c.signal) || split.role;
    ws.addRow({
      rank:     i + 1,
      name:     split.name,
      role:     role,
      company:  split.company,
      domain:   (brief.domain || c.keyword || '').toString().slice(0, 80),
      location: locationOf(c),
      email1:   INTERVIEW_PLACEHOLDER,
      email2:   INTERVIEW_PLACEHOLDER,
      mobile:   INTERVIEW_PLACEHOLDER,
      linkedin: c.url || '',
      website:  extractWebsite(c),
      tcount:   INTERVIEW_PLACEHOLDER,
      tdomain:  INTERVIEW_PLACEHOLDER,
      activity: activityBand(c.score),
      remarks:  buildRemarks(c),
    });
  });

  // Colour the Activity Score column by band
  const activityColIdx = ws.columns.findIndex(c => c.key === 'activity') + 1;
  for (let r = 2; r <= ws.rowCount; r++) {
    const cell = ws.getCell(r, activityColIdx);
    const band = cell.value;
    let fill = 'FFF6E6E8';
    if (band === 'High')   fill = 'FFE8F3EC';
    else if (band === 'Medium') fill = 'FFFBF2E1';
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: fill } };
    ws.getRow(r).alignment = { vertical: 'top', wrapText: true };
  }

  // -------- 2. Engineering details — rubric breakdown for power users --------
  const wsE = wb.addWorksheet('Engineering details');
  wsE.columns = [
    { header: 'Rank',                       key: 'rank',     width: 6 },
    { header: 'Name / Title',               key: 'title',    width: 50 },
    { header: 'Score',                      key: 'score',    width: 7 },
    { header: 'Signal',                     key: 'signal',   width: 18 },
    { header: 'Decision reason',            key: 'why',      width: 40 },
    { header: 'Sonnet rerank',              key: 'sonnet',   width: 40 },
    { header: 'Source URL (verifiable)',    key: 'src',      width: 40 },
    { header: 'Source snippet',             key: 'snip',     width: 60 },
    { header: 'Bucket fit',                 key: 'bf',       width: 9 },
    { header: 'Verify',                     key: 'v',        width: 7 },
    { header: 'Web verify',                 key: 'wv',       width: 32 },
    { header: 'Book',                       key: 'b',        width: 6 },
    { header: 'Tier',                       key: 'tier',     width: 8 },
    { header: 'Sources',                    key: 'sources',  width: 30 },
    { header: 'Keyword',                    key: 'kw',       width: 22 },
  ];
  wsE.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  wsE.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF374151' } };

  accepted.forEach((c, i) => {
    wsE.addRow({
      rank:    i + 1,
      title:   c.title || '(no title)',
      score:   c.score,
      signal:  c.signal,
      why:     c.decision_reason || '',
      sonnet:  c.rerankReason || '',
      src:     c.decision_url || c.url || '',
      snip:    c.decision_snippet || '',
      bf:      c.bucketFit,
      v:       c.verify,
      wv:      c.verifyNote || (c.webVerified == null ? '(not verified)' : ''),
      b:       c.book,
      tier:    c.tier,
      sources: (c.sources || []).join(', '),
      kw:      c.keyword,
    });
  });

  for (let r = 2; r <= wsE.rowCount; r++) {
    const s = wsE.getCell('C' + r).value;
    let fill;
    if (s >= 85) fill = 'FFE8F3EC';
    else if (s >= 65) fill = 'FFEBF0F7';
    else if (s >= 40) fill = 'FFFBF2E1';
    else fill = 'FFF6E6E8';
    wsE.getCell('C' + r).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: fill } };
    wsE.getRow(r).alignment = { vertical: 'top', wrapText: true };
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
    ['INTERVIEW-TIME FIELDS', ''],
    ['Email (Official)',          'Filled by sourcer during the interview call (Step 5 of the manual SOP). Marked "— (interview)" in the Candidates sheet.'],
    ['Email (Personal)',          'Filled by sourcer during the interview call.'],
    ['Mobile Number',             'Filled by sourcer during the interview call.'],
    ['No. of Trainings Conducted','Filled by sourcer during the interview call (asked of the trainer).'],
    ['Relevant Domain Trainings', 'Filled by sourcer during the interview call (asked of the trainer).'],
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

/* ---------- Smart keyword cleanup (v3.5) ----------
   RJP operators paste keywords from anywhere — WhatsApp messages, emails,
   LLM output (ChatGPT/Claude/Perplexity), Word documents, course outlines.
   The 2026-05-02 Splunk Brief 2 had a 12-item "keywords" array with entries
   like "Give the suitable freelance trainer/...", "Configuration via:",
   "Shape", "Forwarders" — clearly TOC fragments and instruction sentences.
   The pipeline ran 12 garbage Google queries and accepted civil engineers
   and chemists as Splunk trainers because keywords like "Shape" matched
   unrelated profiles. Defense in two layers:

   1. Mechanical cleanup — split on newlines, strip bullets/numbering,
      strip trailing colons, strip markdown decorations. Always runs.
   2. Prose detection + LLM extraction — if any remaining keyword looks
      like prose (>12 words, imperative verb, question, instruction
      reference), route the entire raw input through Haiku to extract
      a clean trainer-profile keyword list. ~$0.001 + 6-9s, only fires
      when input was actually messy. */

function mechanicalCleanKeywords(rawKeywords) {
  if (!Array.isArray(rawKeywords)) return [];
  const out = [];
  for (const raw of rawKeywords) {
    if (typeof raw !== 'string') continue;
    // Pasted text often has line breaks within a single "keyword" cell
    const lines = raw.split(/[\n\r]+/);
    for (let line of lines) {
      line = line.trim();
      // Strip leading bullets/numbering: "1.", "1)", "•", "-", "*", "→", "★"
      line = line.replace(/^(?:\d+[.\)]|[•\-\*→★▪·>])\s+/, '').trim();
      // Strip surrounding markdown bold/italic and quotes
      line = line.replace(/^[\*_"'`]+|[\*_"'`]+$/g, '').trim();
      // Strip trailing colon/semicolon (section headers like "Configuration via:")
      line = line.replace(/[:;]+$/, '').trim();
      // Skip empty / single-character noise
      if (!line || line.length < 2) continue;
      out.push(line);
    }
  }
  return out;
}

function looksLikeProse(keyword) {
  if (typeof keyword !== 'string') return false;
  const k = keyword.trim();
  const wordCount = k.split(/\s+/).filter(Boolean).length;
  // Long: > 12 words is almost certainly a sentence not a keyword
  if (wordCount > 12) return true;
  // Imperative / instructional opener
  if (/^\s*(?:Give|Provide|List|Suggest|Recommend|Find|Generate|Create|Tell|Show|Share|Deliver|Return|Fetch|Identify|Need|Looking|Looking for|We need|Please)\b/i.test(k)) return true;
  // Question form
  if (/[?]/.test(k)) return true;
  // Instruction / reference markers in long-ish strings
  if (wordCount > 5 && /\b(?:below|above|TOC|following|the system|the user|hereafter|aforementioned)\b/i.test(k)) return true;
  // Multiple commas in a long-ish keyword = compound clauses
  if (wordCount > 8 && k.split(',').length > 3) return true;
  // Has a colon mid-string (like "Environment: Splunk Cloud Free Trial")
  if (/[^:][:][^:].{3,}/.test(k)) return true;
  return false;
}

async function smartCleanKeywords(rawKeywords, briefDomain, briefTitle, briefId) {
  if (!Array.isArray(rawKeywords) || rawKeywords.length === 0) {
    return { keywords: [], wasMessy: false };
  }
  // Always do mechanical cleanup first
  const mech = mechanicalCleanKeywords(rawKeywords);
  const proseKws = mech.filter(k => looksLikeProse(k));
  const cleanKws = mech.filter(k => !looksLikeProse(k));
  const wasMechanicallyChanged = mech.length !== rawKeywords.length;

  // Happy path: nothing prose-like, return clean (mechanical may have already
  // split and tidied — preserve that as the new keyword list)
  if (proseKws.length === 0 && cleanKws.length > 0) {
    return {
      keywords: cleanKws.slice(0, 8),
      wasMessy: wasMechanicallyChanged,
    };
  }

  // Some keywords look like prose. Route ALL raw input through Haiku to
  // extract trainer-profile keywords from the messy paste.
  const llmAvailable = hasLlmClient();
  if (!llmAvailable) {
    if (briefId) logAndSave(briefId, `[keyword cleanup] dropped ${proseKws.length} prose-like "keyword(s)" — no LLM available for smart parse: ${proseKws.map(k => '"' + k.slice(0, 40) + '..."').join(', ')}`, 'warn');
    return {
      keywords: cleanKws.slice(0, 8),
      wasMessy: true,
      proseDropped: proseKws,
    };
  }

  const client = getAnthropic();
  if (!client) {
    if (briefId) logAndSave(briefId, `[keyword cleanup] LLM client init failed; dropped ${proseKws.length} prose-like keyword(s)`, 'warn');
    return { keywords: cleanKws.slice(0, 8), wasMessy: true, proseDropped: proseKws };
  }

  const blob = rawKeywords.filter(k => typeof k === 'string').join('\n').slice(0, 3000);
  const sys = `You extract TRAINER-PROFILE search keywords from messy pasted text. The input may be: course outlines, WhatsApp messages, email snippets, LLM output (ChatGPT/Claude/Perplexity), Word document fragments, table-of-contents lists, or any unstructured prose.

Return ONLY a JSON array of 4-8 short keywords (each 1-5 words) describing the kind of TRAINER the operator wants to source.

GOOD examples (return shapes like these): "Splunk Observability Trainer", "Salesforce Admin Consultant", "Aurora PostgreSQL DBA", "Cybersecurity SME", "DevOps Solution Architect"

BAD examples (do NOT return these): "Configuration via:", "Forwarders", "below TOC", "40 Hours", "Environment Assumptions", individual technologies that aren't roles, syllabus headers, instruction sentences.

Rules:
- If a domain is given, ATTACH a role suffix (Trainer/Consultant/Architect/SME/Expert/Instructor/Coach) to the domain to form keywords.
- Skip topic/syllabus items, course module names, instructions, and section headers.
- Output JSON array, no commentary.`;

  const user = `Brief title: ${briefTitle || '(none)'}\nBrief domain: ${briefDomain || '(none)'}\nMessy input:\n${blob}\n\nExtract clean trainer-profile keywords. JSON array only.`;

  try {
    const resp = await withRetry(
      () => client.messages.create({
        model: process.env.CLAUDE_MODEL_FAST || 'claude-haiku-4-5-20251001',
        max_tokens: 300,
        system: sys,
        messages: [{ role: 'user', content: user }],
      }),
      { maxAttempts: 2, baseDelayMs: 3000 }
    );
    const text = (resp.content[0] && resp.content[0].text) || '';
    const arr = parseJsonArray(text);
    const clean = arr
      .filter(s => typeof s === 'string')
      .map(s => s.trim())
      .filter(s => s.length >= 2 && s.length <= 80 && !looksLikeProse(s))
      .slice(0, 8);
    if (clean.length === 0) {
      if (briefId) logAndSave(briefId, `[keyword cleanup] LLM returned no usable keywords; falling back to ${cleanKws.length} mechanical-clean keyword(s)`, 'warn');
      return { keywords: cleanKws.slice(0, 8), wasMessy: true, proseDropped: proseKws };
    }
    if (briefId) logAndSave(briefId, `[keyword cleanup] LLM extracted ${clean.length} clean keyword(s) from messy paste: ${clean.map(k => '"' + k + '"').join(', ')}`, 'warn');
    return { keywords: clean, wasMessy: true, proseDropped: proseKws, llmExtracted: true };
  } catch (e) {
    if (briefId) logAndSave(briefId, `[keyword cleanup] LLM cleanup failed (${(e.message || '').slice(0, 80)}); using mechanical-clean keywords (${cleanKws.length})`, 'warn');
    return { keywords: cleanKws.slice(0, 8), wasMessy: true, proseDropped: proseKws };
  }
}

/* ---------- Clarify-input flow (v3.6) ----------
   Conversational pre-submit clarification. The wizard's review step calls
   POST /api/briefs/clarify before letting the operator hit Submit. Returns
   one of three verdicts:

     "clear"               — input is fine, proceed to /api/briefs as today.
     "needs_clarification" — input was messy; here's a draft of what we
                             think you meant + clarifying questions. Operator
                             can confirm-and-submit (with confirmedClean: true)
                             or override-and-submit-original.
     "unsalvageable"       — input is too sparse / too noisy to extract any
                             trainer profile from. Submit is blocked; the
                             operator must refine before submitting.

   When operator confirms a draft, the frontend submits with confirmedClean:
   true and the pipeline skips its own auto-cleaner. When they override,
   the pipeline's cleaner remains as a safety net (legacy behaviour). */

function generateClarifyingQuestions(brief) {
  const qs = [];
  const hasMust    = Array.isArray(brief.must)    && brief.must.length    > 0;
  const hasShould  = Array.isArray(brief.should)  && brief.should.length  > 0;
  const hasMustNot = Array.isArray(brief.mustNot) && brief.mustNot.length > 0;
  const geo = (brief.geo || '').toLowerCase().trim();
  const hasSpecificGeo = geo && geo !== 'india' && geo !== 'all india';

  // Surface only questions whose answer would meaningfully tighten the search.
  if (!hasMust && !hasShould) {
    qs.push('Any specific certifications, years of experience, or skills required? (e.g., "Splunk Core Certified", "8+ years")');
  }
  if (!hasMustNot) {
    qs.push('Anything to AVOID? (e.g., specific firms, certifications, or roles you don\'t want)');
  }
  if (!hasSpecificGeo) {
    qs.push('Specific cities within India, or open to anywhere in India?');
  }
  qs.push('Are you looking for trainers (delivery), consultants (advisory), or both?');
  return qs.slice(0, 3);
}

/* v3.6.1 — single-word and over-long-must detection. Operators sometimes
   submit one-word keywords ("splunk", "trainer", "freelance") which are too
   broad to produce useful Google queries. They also sometimes paste full
   requirement sentences into the must/should/mustNot fields, which the
   pipeline then turns into exact-phrase Google operators that match zero
   profiles. Both cases passed the v3.6 clarify check (looksLikeProse misses
   them) but produce useless output. v3.6.1 catches them and surfaces a
   clarification draft. */

const _GENERIC_SOLO_KEYWORDS = new Set([
  'trainer', 'instructor', 'consultant', 'expert', 'sme', 'architect', 'coach',
  'freelance', 'corporate', 'independent', 'professional', 'specialist',
  'customization', 'configuration', 'setup', 'training', 'observability',
  'cloud', 'devops', 'security', 'data', 'analytics', 'engineer', 'developer',
]);

function detectShortKeywords(keywords) {
  // Returns the keywords that are too short / generic to produce focused queries.
  // Single-word keywords are always flagged (they should at least be 'X Trainer'
  // or 'X Consultant' to give the search a role anchor). Two-word keywords made
  // entirely of generic words ("freelance trainer", "corporate setup") are also
  // flagged. Compound keywords mixing a domain + role ("Splunk Trainer", "AWS
  // Architect") are NOT flagged even though the domain is a single word, because
  // the role suffix anchors the query.
  const out = [];
  for (const k of keywords) {
    if (typeof k !== 'string') continue;
    const words = k.trim().split(/\s+/).filter(Boolean);
    if (words.length === 0) continue;
    if (words.length === 1) { out.push(k); continue; }
    if (words.length === 2) {
      const allGeneric = words.every(w => _GENERIC_SOLO_KEYWORDS.has(w.toLowerCase()));
      if (allGeneric) out.push(k);
    }
  }
  return out;
}

function detectLongMustItems(items, fieldName) {
  // Returns items > 5 words. Must/Should/MustNot are wired to Google as
  // exact-phrase operators (-"...", +"..."), so anything longer than ~3 words
  // matches zero LinkedIn profiles. > 5 words is almost always a misplaced
  // requirement sentence that belongs in the Steering field.
  const out = [];
  for (const it of (items || [])) {
    if (typeof it !== 'string') continue;
    const wc = it.trim().split(/\s+/).filter(Boolean).length;
    if (wc > 5) out.push({ field: fieldName, value: it, wordCount: wc });
  }
  return out;
}

async function clarifyInput(brief, briefId) {
  const rawKeywords = Array.isArray(brief.keywords) ? brief.keywords.filter(k => typeof k === 'string') : [];
  const title  = brief.title  || '';
  const domain = brief.domain || '';
  const roles  = brief.roles  || [];
  const hasUsableRoles = roles.some(r => r && (r.title || r.skill));

  // Total bust: nothing to work with.
  if (rawKeywords.length === 0 && !hasUsableRoles && !domain.trim()) {
    return {
      status: 'unsalvageable',
      reason: 'No keywords, roles, or domain information provided. The brief needs at least one indication of what kind of trainer you are looking for — a technology / domain plus a role hint (Trainer / Consultant / SME / Architect).',
      examples: [
        { input: 'Aurora PostgreSQL DBA Trainer',                    why: 'Clear technology + role.' },
        { input: 'Salesforce Admin Consultant in India, 8+ years',   why: 'Technology + role + filters.' },
        { input: 'Cybersecurity SME with OSCP, freelance, India',    why: 'Domain + role + certification + employment hint.' },
      ],
    };
  }

  // Run smartCleanKeywords (v3.5) — handles messy paste, prose, TOC fragments.
  let cleaned = null;
  let workingKeywords = rawKeywords.slice();
  if (rawKeywords.length > 0) {
    cleaned = await smartCleanKeywords(rawKeywords, domain, title, briefId);

    // Cleaner returned 0 usable keywords — unsalvageable unless we have roles.
    if (cleaned.keywords.length === 0) {
      if (hasUsableRoles) {
        return {
          status: 'needs_clarification',
          issues: [
            `Your keywords field looked like prose / instructions / section headers — none could be turned into searchable keywords. We can still run using your role(s): ${roles.filter(r => r && (r.title || r.skill)).map(r => '"' + (r.title || r.skill) + '"').join(', ')}.`,
          ],
          draft: { keywords: [] },
          questions: generateClarifyingQuestions(brief),
          originalKeywords: rawKeywords,
        };
      }
      return {
        status: 'unsalvageable',
        reason: 'Your keywords field contained only prose / instructions / section headers (no searchable keywords were extractable), and no roles or domain were provided either.',
        examples: [
          { input: 'Splunk Observability Trainer',                why: '1-3 word role with technology.' },
          { input: 'Aurora PostgreSQL DBA',                        why: 'Specific technology + role suffix.' },
          { input: 'AWS Solutions Architect Trainer, 10+ yrs',     why: 'Technology + role + experience hint.' },
        ],
      };
    }

    workingKeywords = cleaned.keywords;
  }

  // v3.5/v3.6.1 — collect issues from all checks.
  const issues = [];
  if (cleaned && cleaned.proseDropped && cleaned.proseDropped.length) {
    const sample = cleaned.proseDropped.slice(0, 3)
      .map(k => '"' + k.slice(0, 60) + (k.length > 60 ? '…' : '') + '"').join(', ');
    issues.push(`${cleaned.proseDropped.length} of your entries looked like prose, instructions, or section headers — not searchable keywords. Examples: ${sample}.`);
  }
  if (rawKeywords.length > 8) {
    issues.push(`You provided ${rawKeywords.length} keyword entries — 4-8 focused keywords typically produce better quality.`);
  }
  if (cleaned && cleaned.llmExtracted) {
    issues.push('I used Haiku to extract trainer-profile keywords from your input.');
  } else if (cleaned && cleaned.wasMessy) {
    issues.push('I tidied newlines, bullets, and trailing colons from your input.');
  }

  // v3.6.1 — single-word / generic short keywords. The pipeline turns each
  // keyword into a Google query; one-word generic keywords like "splunk" or
  // "trainer" produce queries that match millions of irrelevant profiles.
  // The fix is to anchor each with a role suffix.
  const shortKws = detectShortKeywords(workingKeywords);
  let suggestedKeywords = workingKeywords.slice();
  if (shortKws.length > 0) {
    issues.push(`These keywords are too short / generic to focus the search: ${shortKws.map(k => '"' + k + '"').join(', ')}. Each keyword should anchor a domain with a role suffix — e.g., "${domain || 'Splunk'} Trainer", "${domain || 'Splunk'} Consultant", "Freelance ${domain || 'Splunk'} Architect" — instead of single words like "trainer" or "freelance" on their own.`);
    // Build a suggested keyword list: drop the short ones, suggest compound
    // forms using domain + standard role suffixes for any gaps.
    const role_suffixes = ['Trainer', 'Consultant', 'Architect', 'SME'];
    const rest = workingKeywords.filter(k => !shortKws.includes(k));
    const dom = (domain || '').trim();
    if (dom && rest.length < 4) {
      for (const suf of role_suffixes) {
        const cand = `${dom} ${suf}`;
        if (!rest.some(k => k.toLowerCase() === cand.toLowerCase())) rest.push(cand);
        if (rest.length >= 6) break;
      }
    }
    suggestedKeywords = rest.slice(0, 8);
  }

  // v3.6.1 — over-long must/should/mustNot items. These get wired into Google
  // as exact-phrase operators (-"...", +"...") which match zero LinkedIn
  // profiles when over ~3 words. Sentences belong in the Steering field.
  const longMust = [
    ...detectLongMustItems(brief.must,    'Must include'),
    ...detectLongMustItems(brief.should,  'Should include'),
    ...detectLongMustItems(brief.mustNot, 'Must NOT include'),
  ];
  if (longMust.length > 0) {
    const sample = longMust.slice(0, 2)
      .map(x => `${x.field}: "${x.value.slice(0, 70)}${x.value.length > 70 ? '…' : ''}" (${x.wordCount} words)`).join('; ');
    issues.push(`Items in Must/Should/MustNot become exact-phrase Google filters (e.g. \`-"X"\`) that match zero profiles when over ~3 words. ${longMust.length} item(s) are >5 words — move them to the Steering field where the LLM can interpret them as guidance. Examples: ${sample}.`);
  }

  // No issues at all → clear. Otherwise, return the structured clarification.
  if (issues.length === 0) {
    return { status: 'clear' };
  }

  return {
    status: 'needs_clarification',
    issues,
    draft: { keywords: suggestedKeywords },
    questions: generateClarifyingQuestions(brief),
    originalKeywords: rawKeywords,
    ...(longMust.length > 0 ? { longMustItems: longMust } : {}),
  };
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
    // v3.5: Smart keyword cleanup runs BEFORE normalizeBrief so the rest of
    // the pipeline sees only clean keywords. If the operator pasted a course
    // outline, WhatsApp message, LLM output, or any prose into the keywords
    // field, this either mechanical-cleans it (split newlines, strip bullets/
    // colons) or — when prose is detected — asks Haiku to extract proper
    // trainer-profile keywords. Original messy input is preserved on the
    // brief as `originalMessyKeywords` for audit.
    //
    // v3.6: when `confirmedClean: true` is set on the brief (the operator
    // already saw the clarify-endpoint diagnostics in the wizard and chose
    // to proceed with their input), skip the auto-cleaner — they already
    // engaged with the suggestions, no need to run Haiku twice.
    if (Array.isArray(brief.keywords) && brief.keywords.length > 0 && !previewMode && !brief.confirmedClean) {
      const stopClean = stageStart('Keyword cleanup');
      const cleaned = await smartCleanKeywords(brief.keywords, brief.domain, brief.title, briefId);
      stopClean();
      if (cleaned.wasMessy) {
        if (cleaned.keywords.length === 0) {
          // Hard fail with a clear error rather than silently producing garbage
          const reason = 'Your keywords field contained only prose / instructions / section headers — no usable trainer-profile keywords could be extracted. Please provide 1-8 short keywords (1-5 words each) describing the trainer profile, e.g. "Splunk Observability Trainer", "Salesforce Admin Consultant".';
          store.update(briefId, { status: 'failed', error: reason, retryable: false, originalMessyKeywords: brief.keywords });
          logAndSave(briefId, reason, 'err');
          return;
        }
        store.update(briefId, {
          keywords: cleaned.keywords,
          originalMessyKeywords: brief.keywords,
        });
        brief.keywords = cleaned.keywords;
      }
    }

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

    // ---- PHASE 1: top-of-stack named trainers (Claude knowledge) — v3.9 framing ----
    // Ramesh's Phase 1: "the perfect match, the curated list at the top."
    let allItems = [];
    if (!previewMode && hasLlmClient()) {
      const stop = stageStart('Phase 1: Claude knowledge');
      for (const kw of bp.keywords) {
        const items = await claudeKnowledgeCall(briefId, kw, bp);
        logAndSave(briefId, `[Phase 1] Claude knowledge for "${kw}" → ${items.length} candidates`);
        allItems = allItems.concat(items);
      }
      stop();
    } else if (!previewMode) {
      logAndSave(briefId, `Phase 1 skipped — no LLM client (set ANTHROPIC_VIA_CLAUDE_CLI=true or ANTHROPIC_API_KEY). Falling back to Google-only.`, 'warn');
    }

    // ---- PHASE 1.2: live-web named-trainer probe (Perplexity Sonar Pro) — v3.11.0 ----
    // Sits between Phase 1 (Claude knowledge prior) and Phase 2 (Google source-
    // mining). Same named-trainer ask, but Sonar Pro grounds the answer in live
    // web search results — picks up trainers active after the Claude cutoff,
    // recent course launches, fresh meetup organisers, and platforms Claude
    // indexes weakly. ~$0.015 per keyword. Gated behind PERPLEXITY_API_KEY:
    // skipped silently in the pipeline log when the key is unset, so this
    // ships safely ahead of key provisioning.
    if (!previewMode && hasPerplexity()) {
      const stop = stageStart('Phase 1.2: Perplexity web');
      for (const kw of bp.keywords) {
        const items = await perplexityKnowledgeCall(briefId, kw, bp);
        logAndSave(briefId, `[Phase 1.2] Perplexity web for "${kw}" → ${items.length} candidates`);
        allItems = allItems.concat(items);
      }
      stop();
    } else if (!previewMode) {
      logAndSave(briefId, `[Phase 1.2] skipped — set PERPLEXITY_API_KEY to enable live-web named-trainer search`, 'info');
    }

    // ---- PHASE 2: keyword-combination expansion (Apify Google) — v3.9 framing ----
    // Ramesh's Phase 2: "deeper combinations of keywords." 11 source toggles
    // get interleaved per keyword (LinkedIn, UrbanPro, blogs, YouTube, Udemy,
    // Indian platforms, authority directories, course platforms, Meetup,
    // Eventbrite, GitHub-edu) plus domain-specific authority queries.
    const stopGoogle = stageStart('Phase 2: Google search');
    const queries = buildGoogleQueries(brief, bp);
    const queriesToRun = previewMode ? queries.slice(0, 1) : queries;
    logAndSave(briefId, `[Phase 2] Built ${queries.length} queries; running ${queriesToRun.length}${previewMode ? ' (preview)' : ''}`);
    for (let i = 0; i < queriesToRun.length; i++) {
      const q = queriesToRun[i];
      logAndSave(briefId, `[Phase 2 ${i + 1}/${queriesToRun.length}] ${q.query.slice(0, 110)}`);
      const items = await runGoogleQuery(apifyClient, q, briefId);
      logAndSave(briefId, `[Phase 2 ${i + 1}/${queriesToRun.length}] +${items.length} items`);
      allItems = allItems.concat(items);
      store.update(briefId, { counts: { ...((store.get(briefId) || {}).counts || {}), discovered: allItems.length } });
    }
    stopGoogle();

    // ---- PHASE 3a: adjacent-tech expansion (Niche or thin results) ----
    // Ramesh's Phase 3 first leg: adjacent technologies whose practitioners
    // could plausibly deliver the target.
    let normalised = dedupeItems(allItems.map(normaliseItem));
    if (!previewMode && (bp.searchMode === 'niche' || normalised.length < 10) && hasLlmClient()) {
      const stop = stageStart('Phase 3a: Adjacent tech');
      for (const kw of bp.keywords) {
        const adj = await claudeAdjacentTech(briefId, kw, bp);
        logAndSave(briefId, `[Phase 3a] Adjacent tech for "${kw}": ${adj.join(', ') || '(none)'}`);
        for (const a of adj) {
          // Run a single LinkedIn query per adjacent tech
          const q = { query: `independent instructor ${a} India site:linkedin.com/in`, keyword: a, source: 'linkedin', tier: 'L2' };
          const items = await runGoogleQuery(apifyClient, q, briefId);
          logAndSave(briefId, `[Phase 3a] "${a}" → +${items.length} items`);
          allItems = allItems.concat(items);
        }
      }
      normalised = dedupeItems(allItems.map(normaliseItem));
      stop();
    }

    // ---- PHASE 3b: Indian training institutes (Niche + still thin) ----
    if (!previewMode && bp.searchMode === 'niche' && normalised.length < 8 && hasLlmClient()) {
      const stop = stageStart('Phase 3b: Institutes');
      for (const kw of bp.keywords) {
        const inst = await claudeInstitutes(briefId, kw, bp);
        logAndSave(briefId, `[Phase 3b] Institutes for "${kw}" → ${inst.length}`);
        allItems = allItems.concat(inst);
      }
      normalised = dedupeItems(allItems.map(normaliseItem));
      stop();
    }

    // ---- PHASE 3c: founders / principals at small Indian firms (v3.9.0) ----
    // Ramesh's Phase 3 final leg from the call: "alternative people — founders,
    // principals at small firms specialising in the tech." Fires for niche
    // briefs OR when L1+L2+L3 produced thin results (<12 candidates total).
    // High-signal because these people RUN the firm — they decide whether to
    // accept training engagements, and their delivery depth matches Architect+
    // tier without the "Architect = reject" issue Saranya flagged.
    if (!previewMode && (bp.searchMode === 'niche' || normalised.length < 12) && hasLlmClient()) {
      const stop = stageStart('Phase 3c: Founders');
      for (const kw of bp.keywords) {
        const founders = await claudeFoundersAtSmallFirms(briefId, kw, bp);
        logAndSave(briefId, `[Phase 3c] Founders/principals for "${kw}" → ${founders.length}`);
        allItems = allItems.concat(founders);
      }
      normalised = dedupeItems(allItems.map(normaliseItem));
      stop();
    }

    logAndSave(briefId, `${normalised.length} unique items across all phases (1+2+3a/b/c)`);

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

    // v3.5: Hard-exclusion safety net. The LLM classifier sometimes hallucinates
    // past explicit BIG_FIRM / NON_INDIA / MUSTNOT_HIT rules — observed on
    // 2026-05-02 Splunk Brief 1 where 4 TCS/Infosys current employees were
    // classified PRACTITIONER instead of BIG_FIRM despite the explicit rule
    // in the system prompt. Re-run regex on every non-rejected candidate;
    // if regex says hard-reject, override the LLM verdict. Regex is the
    // authoritative source on these three rules — operators MUST be able
    // to trust customExclusions / mustNot / India-only filters.
    let safetyOverrides = 0;
    for (let i = 0; i < classifications.length; i++) {
      const cls = classifications[i];
      if (!cls || REJECTED_SIGNALS.has(cls.signal)) continue;
      const { it, harvest } = itemsWithHarvest[i];
      const regexCls = classifySignalRegex(it, bp, harvest);
      if (regexCls.signal === 'BIG_FIRM' || regexCls.signal === 'NON_INDIA' || regexCls.signal === 'MUSTNOT_HIT') {
        classifications[i] = {
          signal: regexCls.signal,
          reason: `${regexCls.reason} [safety override of LLM '${cls.signal}']`,
        };
        safetyOverrides++;
      }
    }
    if (safetyOverrides > 0) {
      logAndSave(briefId, `[classifier safety] regex overrode ${safetyOverrides} LLM verdict(s) on hard-exclude rules (BIG_FIRM/NON_INDIA/MUSTNOT_HIT)`, 'warn');
    }

    // v3.7: Signal-vs-reason consistency check. Haiku occasionally returns
    // a positive signal (TRAINER_EXPLICIT / FREELANCE_TRAINER / etc.) AND a
    // reason text that contradicts it ("no training", "rejected — out of
    // domain", "non-Netbrain focus"). Observed on the 2026-05-02 Netbrain
    // and Splunk-1 reruns: 4 candidates accepted as TRAINER_EXPLICIT despite
    // reasons literally saying "rejected — out of Netbrain". The signal
    // alone determines accept/reject in compositeScore, so contradictions
    // slip into the final list and pollute output.
    //
    // Detection: scan each non-rejected reason for negation phrases near
    // training/domain terms. If found, demote to NOISE (rejected). Defends
    // against LLM contradicting itself when steering is strict.
    //
    // v3.7.1 (2026-05-02 same day): broaden the regex after observing the
    // post-v3.7 Splunk-1 retry STILL letting through 2/3 noise candidates
    // with reasons "REJECT: DevSecOps/AIOps, no observability tooling depth"
    // and "REJECT: Pure Azure cloud, no observability focus". Three gaps:
    //   1. `REJECT:` (no -ed) — original pattern only caught `rejected:`
    //   2. `no observability tooling depth` — original needed the term right
    //      after `no\s+`; the broader `no \w+ \w+ (depth|focus|...)` form was
    //      missing. Now matches 1-3 words between "no" and the qualifier.
    //   3. `Pure Azure cloud, no <X>` — uppercase-led "REJECT-like" phrases.
    const NEGATION_RE = new RegExp([
      // Reason starts or contains an explicit reject/not-a-X verdict label
      String.raw`\breject(?:s|ed)?\s*[:\-—]`,                                       // REJECT:, rejects:, rejected—
      String.raw`^\s*reject\b`,                                                      // bare leading "REJECT"
      String.raw`\bnot\s+(?:a\s+)?(?:trainer|relevant|in[\-\s]?domain|netbrain|splunk|salesforce|aws|domain)\b`,
      String.raw`\bout\s+of\s+(?:domain|netbrain|splunk|aws|salesforce|scope|focus)\b`,
      // "no <1-3 words> {qualifier}" — generalised negation near a quality term
      String.raw`\bno\s+(?:\w+\s+){0,3}(?:training|teaching|expertise|experience|relevance|evidence|focus|depth|tooling|knowledge|background)\b`,
      // "without <X> {qualifier}"
      String.raw`\bwithout\s+(?:\w+\s+){0,3}(?:depth|expertise|experience|training|focus|knowledge|tooling|background)\b`,
      // "lacks <X> {qualifier}"
      String.raw`\blacks?\s+(?:\w+\s+){0,3}(?:depth|expertise|experience|training|focus|knowledge|tooling|background)\b`,
      // "non-X focus" / "non-domain focus"
      String.raw`\bnon[\-\s]\w+\s+focus\b`,
      // "skilled but no <X>" — common LLM contradiction pattern
      String.raw`\bskilled\s+but\s+no\s+(?:training|relevance|experience|teaching)\b`,
      // "Pure <X>, no <Y>" — explicit out-of-domain framing
      String.raw`\bpure\s+\w+(?:[\s,]\w+){0,3},?\s*no\s+\w+\b`,
      // "out of scope" / "out-of-scope"
      String.raw`\bout[\-\s]of[\-\s]scope\b`,
    ].join('|'), 'i');
    let consistencyOverrides = 0;
    for (let i = 0; i < classifications.length; i++) {
      const cls = classifications[i];
      if (!cls || REJECTED_SIGNALS.has(cls.signal)) continue;
      if (cls.reason && NEGATION_RE.test(cls.reason)) {
        classifications[i] = {
          signal: 'NOISE',
          reason: `[demoted by consistency check — reason text negated the positive signal] Original: ${cls.reason}`,
        };
        consistencyOverrides++;
      }
    }
    if (consistencyOverrides > 0) {
      logAndSave(briefId, `[classifier consistency] ${consistencyOverrides} candidate(s) demoted to NOISE because the LLM's reason text contradicted its own positive signal (e.g., "no training", "rejected — out of <domain>", "non-X focus")`, 'warn');
    }

    const scored = normalised.map((it, i) => {
      const harvest = itemsWithHarvest[i].harvest;
      const cls = classifications[i];
      const bf = bucketFit(it, bp.keywords);
      const v = verifyScore(it, harvest);
      const bk = bookScore(cls.signal);
      const candidate = {
        ...it,
        // v3.4: carry harvest through to the candidate so buildXlsx can read
        // location/headline/experience/about for the spec-column Excel. Harvest
        // is null for non-LinkedIn entries (Udemy, YouTube, blog hits) — the
        // helpers (splitNameRoleCompany, locationOf, extractWebsite) handle
        // that case by falling back to title/text scraping.
        harvest: harvest || null,
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

    // v3.9.0 — Iteration summary (Vijay's "send him iteration history & best
    // matches" ask). Compute top-5 name diff vs the prior run on this brief
    // so the detail view can render an iteration trail. _prevTopNames is the
    // PRIOR run's top-5; we save the CURRENT top-5 there for the next rerun.
    const refreshedBrief = store.get(briefId) || brief;
    const currentTopNames = capped.slice(0, 5).map(c => {
      try { return splitNameRoleCompany(c).name; } catch { return ''; }
    }).filter(Boolean);
    const prevTopNames = Array.isArray(refreshedBrief._prevTopNames) ? refreshedBrief._prevTopNames : [];
    const prevSet = new Set(prevTopNames.map(n => n.toLowerCase()));
    const currentSet = new Set(currentTopNames.map(n => n.toLowerCase()));
    const newlyEntered = currentTopNames.filter(n => !prevSet.has(n.toLowerCase()));
    const dropped = prevTopNames.filter(n => !currentSet.has(n.toLowerCase()));
    const lastFeedback = (refreshedBrief.feedbackHistory || []).slice(-1)[0];
    const summaryEntry = {
      rev: refreshedBrief.feedbackRevisions || 0,
      ts: new Date().toISOString(),
      candidateCount: capped.length,
      topNames: currentTopNames,
      newlyEntered,
      dropped,
      feedbackApplied: lastFeedback ? String(lastFeedback.text || '').slice(0, 400) : null,
      lowYield,
    };
    const iterationSummary = (refreshedBrief.iterationSummary || []).concat([summaryEntry]);

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
      iterationSummary,
      _prevTopNames: currentTopNames,
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

/* ---------- Boot reaper (v3.2): catch orphans from a previous-life dyno ---------- */
// Render free tier spins down after 15 min idle and restarts on traffic.
// If a dyno restart happens mid-pipeline, the brief is left in a non-terminal
// state in Postgres but no process is running it — it's an orphan. The 25-min
// watchdog will eventually catch it but that's a slow recovery.
//
// On boot, any brief in {queued, discovery, scoring, packaging, preview} MUST
// be an orphan: the only Node process that could update its state is THIS
// process, which just started. Mark them failed immediately with
// retryable:true so the Retry button does the right thing for RJP.
function reapOrphansOnBoot() {
  const now = new Date().toISOString();
  let reaped = 0;
  for (const b of store.list()) {
    if (!NON_TERMINAL_STATUSES.has(b.status)) continue;
    const reason = `Pipeline orphaned by server restart. The brief was in '${b.status}' state when the dyno restarted; no process is now running it. Click Retry to re-run.`;
    store.update(b.id, {
      status: 'failed',
      error: reason,
      retryable: true,
      log: (b.log || []).concat([{ ts: now, msg: reason, kind: 'err' }]),
    });
    reaped++;
    console.warn(`[boot reaper] auto-failed orphan ${b.id} (was in ${b.status})`);
  }
  if (reaped) console.log(`[boot reaper] marked ${reaped} orphan(s) as failed.`);
}

module.exports = { runPipeline, runWithFeedback, startWatchdog, reapOrphansOnBoot, OUTPUT_DIR, DEFAULT_BIGFIRM_EXCLUSIONS, clarifyInput };
