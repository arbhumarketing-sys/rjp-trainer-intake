/**
 * Sourcing pipeline.
 *
 * 1. Build queries from the brief (domain, roles, geo, steering hints).
 * 2. Run Apify rag-web-browser actor (or actor specified by APIFY_ACTOR env).
 * 3. Pull dataset items, dedupe, normalise into candidate rows.
 * 4. Apply provisional v2 rubric scoring (conservative defaults — searcher tightens later).
 * 5. Build .xlsx (and .pdf if requested) and save to OUTPUT_DIR.
 */
const fs = require('fs');
const path = require('path');
const { ApifyClient } = require('apify-client');
const ExcelJS = require('exceljs');
const store = require('./store');

const OUTPUT_DIR = process.env.OUTPUT_DIR || path.join(__dirname, 'outputs');
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

const APIFY_ACTOR = process.env.APIFY_ACTOR || 'apify~rag-web-browser';
const MAX_QUERIES = parseInt(process.env.MAX_QUERIES_PER_BRIEF || '12', 10);
const MAX_RESULTS = parseInt(process.env.MAX_RESULTS_PER_QUERY || '20', 10);

const SIGNAL_POINTS = { EXPLICIT: 40, FREELANCE: 30, INSTITUTE: 20, PRACTITIONER: 10 };
const TRAINER_KEYWORDS = ['trainer', 'instructor', 'corporate trainer', 'training', 'mentor', 'faculty', 'coach', 'workshop'];
const FREELANCE_KEYWORDS = ['freelance', 'independent', 'consultant', 'self-employed'];
const INSTITUTE_KEYWORDS = ['academy', 'institute', 'training center', 'training centre', 'learning', 'cohort', 'bootcamp'];
const INDIA_KEYWORDS = ['india', 'bengaluru', 'bangalore', 'mumbai', 'pune', 'hyderabad', 'chennai', 'delhi', 'gurgaon', 'noida', 'kolkata', 'ahmedabad'];

function logAndSave(briefId, msg, kind = 'info') {
  console.log('[pipeline:' + briefId + ']', msg);
  store.appendLog(briefId, msg, kind);
}

/* ---------- Query builder ---------- */
function buildQueries(brief) {
  const domain = (brief.domain || '').trim();
  const geo = (brief.geo || 'India').trim();
  const queries = [];

  // One query per role × site bucket
  const sites = [
    { tag: 'linkedin', op: 'site:in.linkedin.com OR site:linkedin.com/in' },
    { tag: 'urbanpro', op: 'site:urbanpro.com OR site:sulekha.com' },
    { tag: 'youtube',  op: 'site:youtube.com' },
    { tag: 'general',  op: '' },
  ];

  for (const role of brief.roles || []) {
    const seed = [domain, role.title, role.skill].filter(Boolean).join(' ');
    for (const s of sites) {
      const q = [`"${seed}" trainer`, geo, s.op].filter(Boolean).join(' ');
      queries.push({ query: q, role: role.title, site: s.tag });
      if (queries.length >= MAX_QUERIES) break;
    }
    if (queries.length >= MAX_QUERIES) break;
  }

  // Steering hints can shape queries
  const steering = (brief.steering || '').toLowerCase();
  if (queries.length < MAX_QUERIES && steering) {
    if (steering.includes('certification') || steering.includes('certified')) {
      queries.push({ query: `"${domain}" certified trainer ${geo}`, role: 'cert-pass', site: 'general' });
    }
    if (steering.includes('youtube') || steering.includes('video')) {
      queries.push({ query: `"${domain}" tutorial ${geo} site:youtube.com`, role: 'video-pass', site: 'youtube' });
    }
  }

  return queries.slice(0, MAX_QUERIES);
}

/* ---------- Apify run ---------- */
async function runApifyQuery(client, q) {
  // Dev mode: don't burn credits while wiring up.
  if (process.env.MOCK_APIFY === '1') {
    return mockResults(q);
  }
  const input = {
    query: q.query,
    maxResults: MAX_RESULTS,
    outputFormats: ['markdown'],
    scrapingTool: 'raw-http',
    requestTimeoutSecs: 30,
  };
  const run = await client.actor(APIFY_ACTOR).call(input, { waitSecs: 180 });
  if (!run || !run.defaultDatasetId) return [];
  const dataset = await client.dataset(run.defaultDatasetId).listItems();
  return (dataset.items || []).map(it => ({ ...it, _query: q.query, _site: q.site, _role: q.role }));
}

function mockResults(q) {
  // Synthetic results for local dev. Six per query, varied signal/geo to exercise scoring.
  const base = [
    { name: 'Aarti Sharma',  role: 'Senior Salesforce Trainer · Bengaluru', linkedin: 'in/aarti-sharma-sfdc',  trainer: true,  freelance: false, india: true,  score: 'high' },
    { name: 'Rahul Mehta',   role: 'Independent Salesforce Consultant',     linkedin: 'in/rahul-mehta-sf',     trainer: true,  freelance: true,  india: true,  score: 'high' },
    { name: 'Priya Iyer',    role: 'Salesforce Architect · Mumbai',         linkedin: 'in/priya-iyer-arch',    trainer: false, freelance: false, india: true,  score: 'mid' },
    { name: 'Edge Tech Academy', role: 'Salesforce Training Institute',     linkedin: 'company/edge-tech',     trainer: false, freelance: false, india: true,  score: 'mid', institute: true },
    { name: 'Vikram Banerjee', role: 'Cloud Engineer · Pune',               linkedin: 'in/vikram-b',           trainer: false, freelance: false, india: true,  score: 'low' },
    { name: 'Carlos Ramirez', role: 'Salesforce Trainer · Madrid',          linkedin: 'in/carlos-r',           trainer: true,  freelance: false, india: false, score: 'zero' },
  ];
  return base.map((p, i) => ({
    url: 'https://linkedin.com/' + p.linkedin,
    metadata: { title: `${p.name} — ${p.role}` },
    markdown: `${p.name}. ${p.role}. ${p.trainer ? 'corporate trainer' : 'platform engineer'}. ${p.freelance ? 'freelance independent consultant' : ''} ${p.institute ? 'training institute' : ''} ${p.india ? 'India' : 'Spain'}. ${(q.role || '').toLowerCase()} ${(q.query || '').toLowerCase()}`,
    _query: q.query,
    _site: q.site,
    _role: q.role,
    _mockIdx: i,
  }));
}

/* ---------- Item normalisation + scoring ---------- */
function normalise(item) {
  const url = item.url || item.searchResult?.url || '';
  const title = (item.metadata?.title || item.searchResult?.title || item.title || '').trim();
  const text = (item.markdown || item.text || item.searchResult?.description || '').slice(0, 2000);
  return {
    url,
    title,
    text,
    site: item._site,
    role: item._role,
    query: item._query,
  };
}

function classifySignal(it) {
  const blob = (it.title + ' ' + it.text).toLowerCase();
  if (TRAINER_KEYWORDS.some(k => blob.includes(k))) {
    if (FREELANCE_KEYWORDS.some(k => blob.includes(k))) return 'FREELANCE';
    if (INSTITUTE_KEYWORDS.some(k => blob.includes(k))) return 'INSTITUTE';
    return 'EXPLICIT';
  }
  if (INSTITUTE_KEYWORDS.some(k => blob.includes(k))) return 'INSTITUTE';
  return 'PRACTITIONER';
}

function geoIN(it) {
  const blob = (it.title + ' ' + it.text + ' ' + it.url).toLowerCase();
  return INDIA_KEYWORDS.some(k => blob.includes(k));
}

function bucketFit(it, domain) {
  const d = (domain || '').toLowerCase();
  if (!d) return 2;
  const blob = (it.title + ' ' + it.text).toLowerCase();
  const hits = (blob.match(new RegExp(d, 'g')) || []).length;
  if (hits >= 3) return 3;
  if (hits >= 1) return 2;
  return 1;
}

function verifyScore(it) {
  // Strong: LinkedIn + a 2nd source. Med: LinkedIn-only. Low: neither.
  if (it.site === 'linkedin' && it.url.includes('linkedin.com/in/')) return 2;
  if (it.url) return 2;
  return 1;
}

function bookScore(signal) {
  if (signal === 'INSTITUTE') return 3;
  if (signal === 'FREELANCE') return 2;
  if (signal === 'EXPLICIT') return 2;
  return 1;
}

function score(p) {
  if (!p.geoIndia) return 0;
  return SIGNAL_POINTS[p.signal] + (p.bucketFit * 10) + (p.verify * 5) + (p.book * 5);
}

function dedupe(items) {
  const seen = new Set();
  const out = [];
  for (const it of items) {
    const key = (it.url || it.title || '').toLowerCase().slice(0, 200);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }
  return out;
}

/* ---------- Excel build ---------- */
async function buildXlsx(brief, candidates) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'RJP Sourcing Portal';
  wb.created = new Date();

  const ws = wb.addWorksheet('Candidates');
  ws.columns = [
    { header: 'Rank', key: 'rank', width: 6 },
    { header: 'Score', key: 'score', width: 7 },
    { header: 'Title / Headline', key: 'title', width: 50 },
    { header: 'URL', key: 'url', width: 60 },
    { header: 'Signal', key: 'signal', width: 14 },
    { header: 'Bucket Fit', key: 'bucketFit', width: 10 },
    { header: 'Verify', key: 'verify', width: 8 },
    { header: 'Book', key: 'book', width: 7 },
    { header: 'Geo IN', key: 'geoIndia', width: 8 },
    { header: 'Source query', key: 'query', width: 40 },
    { header: 'Role tag', key: 'role', width: 24 },
    { header: 'Site', key: 'site', width: 12 },
    { header: 'Snippet', key: 'text', width: 80 },
  ];
  ws.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  ws.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF2A4365' } };
  ws.getRow(1).alignment = { vertical: 'middle' };

  candidates.forEach((c, i) => {
    ws.addRow({
      rank: i + 1,
      score: c.score,
      title: c.title || '(no title)',
      url: c.url,
      signal: c.signal,
      bucketFit: c.bucketFit,
      verify: c.verify,
      book: c.book,
      geoIndia: c.geoIndia ? 'IN' : 'OUT',
      query: c.query,
      role: c.role,
      site: c.site,
      text: c.text,
    });
  });

  // Banded fills
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
  ws.getColumn('text').alignment = { wrapText: true, vertical: 'top' };

  // Brief context tab — surface steering text and roles
  const wsCtx = wb.addWorksheet('Brief context');
  wsCtx.columns = [{ header: 'Field', key: 'k', width: 22 }, { header: 'Value', key: 'v', width: 80 }];
  wsCtx.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
  wsCtx.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF2A4365' } };
  const ctxRows = [
    ['Title', brief.title],
    ['Domain', brief.domain],
    ['Geography', brief.geo],
    ['Deadline', brief.deadline || '—'],
    ['Output format', brief.outputFormat],
    ['Submitted at', brief.createdAt],
    ['Brief ID', brief.id],
    ['', ''],
    ['STEERING / SPECIFIC DIRECTION', ''],
    ['', brief.steering || '(none provided)'],
    ['', ''],
    ['ROLES', ''],
    ...((brief.roles || []).map((r, i) => [`${i + 1}. ${r.title}`,
      [r.skill && `skill: ${r.skill}`, r.bucket && `bucket: ${r.bucket}`, r.count && `count: ${r.count}`].filter(Boolean).join(' · ')
    ])),
    ['', ''],
    ['SCORING RUBRIC', ''],
    ['Signal',    'EXPLICIT 40 | FREELANCE 30 | INSTITUTE 20 | PRACTITIONER 10'],
    ['Bucket fit','1-3 × 10'],
    ['Verify',    '1-3 × 5'],
    ['Book',      '1-3 × 5  (institute > freelancer > FTE)'],
    ['Geo',       'India IN passes / OUT scores 0'],
    ['',          ''],
    ['NOTE',      'Scores are provisional — auto-classified from scrape. Searcher should verify and adjust before final delivery.'],
  ];
  ctxRows.forEach(r => wsCtx.addRow({ k: r[0], v: r[1] }));
  wsCtx.eachRow(r => r.alignment = { vertical: 'top', wrapText: true });

  const file = path.join(OUTPUT_DIR, brief.id + '.xlsx');
  await wb.xlsx.writeFile(file);
  return file;
}

/* ---------- Top-level pipeline ---------- */
async function runPipeline(briefId) {
  const brief = store.get(briefId);
  if (!brief) return;
  const apifyToken = process.env.APIFY_TOKEN;
  if (!apifyToken) {
    store.update(briefId, { status: 'failed', error: 'APIFY_TOKEN not set on server' });
    logAndSave(briefId, 'APIFY_TOKEN not configured', 'err');
    return;
  }
  const client = new ApifyClient({ token: apifyToken });

  try {
    store.update(briefId, { status: 'running', counts: {} });
    logAndSave(briefId, 'Pipeline starting');

    const queries = buildQueries(brief);
    logAndSave(briefId, `Built ${queries.length} search queries`);

    let allItems = [];
    for (let i = 0; i < queries.length; i++) {
      const q = queries[i];
      logAndSave(briefId, `[${i + 1}/${queries.length}] running query: ${q.query.slice(0, 100)}`);
      try {
        const items = await runApifyQuery(client, q);
        logAndSave(briefId, `[${i + 1}/${queries.length}] +${items.length} items`);
        allItems = allItems.concat(items);
        store.update(briefId, { counts: { ...((store.get(briefId) || {}).counts || {}), discovered: allItems.length } });
      } catch (e) {
        logAndSave(briefId, `query failed: ${e.message}`, 'err');
      }
    }

    const normalised = dedupe(allItems.map(normalise));
    logAndSave(briefId, `${normalised.length} unique items after dedupe`);

    store.update(briefId, { status: 'scoring', counts: { ...((store.get(briefId) || {}).counts || {}), scored: 0 } });
    const scored = normalised.map(it => {
      const signal = classifySignal(it);
      const geoIndia = geoIN(it);
      const bf = bucketFit(it, brief.domain);
      const v = verifyScore(it);
      const bk = bookScore(signal);
      const candidate = { ...it, signal, bucketFit: bf, verify: v, book: bk, geoIndia };
      candidate.score = score(candidate);
      return candidate;
    }).sort((a, b) => b.score - a.score);

    logAndSave(briefId, `Scored ${scored.length} candidates`);
    store.update(briefId, { counts: { ...((store.get(briefId) || {}).counts || {}), scored: scored.length } });

    store.update(briefId, { status: 'building' });
    const file = await buildXlsx(brief, scored);
    const stat = fs.statSync(file);
    logAndSave(briefId, `Built xlsx (${(stat.size / 1024).toFixed(1)} KB) → ${path.basename(file)}`);

    store.update(briefId, {
      status: 'complete',
      outputFile: path.basename(file),
      counts: { ...((store.get(briefId) || {}).counts || {}), rows: scored.length },
      completedAt: new Date().toISOString(),
    });
    logAndSave(briefId, 'Pipeline complete');
  } catch (e) {
    logAndSave(briefId, 'Pipeline failed: ' + e.message, 'err');
    store.update(briefId, { status: 'failed', error: e.message });
  }
}

module.exports = { runPipeline, OUTPUT_DIR };
