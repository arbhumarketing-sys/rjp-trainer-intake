# RJP Infotek — Trainer Sourcing Portal (v3.3)

Production trainer-sourcing pipeline for RJP Infotek (rjpinfotek.com).
Last major release: **2026-05-01 / 2026-05-02 → v3.3**.

This README doubles as the start-of-chat handoff prompt — paste it as the
first message in any new conversation about this project to reload context.

---

## LIVE URLS

```
Frontend:  https://rjp-trainer-intake.vercel.app
Server:    https://rjp-trainer-intake-server.onrender.com
Repo:      https://github.com/arbhumarketing-sys/rjp-trainer-intake (main)
```

`/healthz` JSON reports `version: 3.2`. Tip commit: `7a3dda5`.

## ARCHITECTURE

- Frontend: single `index.html` on Vercel — auto-deploys on push (Vercel webhook works fine)
- Server: Node/Express on Render free tier, region `singapore`
- Storage: Render free Postgres + in-memory cache (write-through). Excel
  bytes also persisted to Postgres (`brief_outputs` table) so downloads
  survive dyno restarts
- LLM: `claude` CLI subprocess on Render, OAuth-auth'd, billed against the
  operator's Claude Max subscription. **No API console spend.**
- Apify Google search (`apify~rag-web-browser`) + harvestapi LinkedIn for
  discovery + enrichment
- Global CLI semaphore (`MAX_CONCURRENT_CLI=2`) caps concurrent `claude`
  subprocesses across the whole Node process — prevents Render free-tier
  OOM thrash when multiple briefs run simultaneously

## CREDENTIALS / FILES (operator's Mac)

```
Local clone:               ~/dev/rjp-trainer-intake/
Cowork (source-of-truth):  ~/Documents/Claude-Files/outputs/Claude Cowork/CLAUDE OUTPUTS/RJPInfotek/client-portal/
~/.config/rjp/render-api-key       Render API key (mode 600)
~/.config/rjp/database-url         Postgres internal URL (mode 600)
~/.config/rjp/claude-oauth-token   1-year Claude Max OAuth token (mode 600)
~/.config/rjp/apify-token          Apify token (mode 600)
~/bin/rjp-deploy.sh                One-shot deploy script

Render service id:    srv-d7pknavavr4c73erkobg
Render Postgres id:   dpg-d7q2875ckfvc739f446g-a
GitHub Actions repo secrets (already set): RENDER_API_KEY, RENDER_SERVICE_ID
gh auth: arbhumarketing-sys (HTTPS, with `workflow` scope after 2026-05-01 refresh)
```

## HOW TO DEPLOY

GitHub Actions handles every deploy since 2026-05-01. The previous
GitHub→Render webhook was unreliable; replaced with explicit Action.

```bash
# 1. Edit files in Cowork (source-of-truth, including .github/workflows/)
# 2. Run:
rjp-deploy.sh "what changed"
# 3. Script: rsync Cowork→repo → commit → push → wait for GH Actions
#    → Action calls Render API + polls until live + verifies healthz
#    → Script tails the Action via `gh run watch` and exits 0/1
```

Failed deploys turn the commit red and email the repo owner. No silent
failures.

GitHub Actions workflows in `.github/workflows/`:
- `render-deploy.yml` — triggers + verifies Render deploy on push to main
- `keep-warm.yml` — pings `/healthz` every 10 min (avoids cold starts)

## ENGINE FEATURES (v3.3)

### Discovery cascade

- **L1.1** Claude knowledge (per keyword)
- **L1.3** Apify Google search across 11 source toggles (interleaved per keyword)
- **L2** Claude adjacent-tech expansion (when niche or thin)
- **L3** Claude Indian institutes (when niche + still thin)
- **L4** YouTube + Udemy + Meetup + Eventbrite + GitHub mining
- **harvestapi-linkedin enrichment** (cap 60 profiles/brief, ~$0.24)

### 11 source toggles in wizard step 3 (all default ON)

- LinkedIn (`site:linkedin.com/in`)
- UrbanPro / Sulekha (Indian marketplaces)
- Tech blogs (Hashnode, Dev.to, Medium, Substack)
- YouTube tutorials
- Udemy courses
- **Indian training platforms** — Edureka, Simplilearn, GreatLearning, UpGrad, AnalyticsVidhya, Whizlabs, KodeKloud
- **Authority directories** — MS MVP, Google GDE, AWS Hero, Salesforce Trailblazer, CNCF Ambassador
- **Course platforms** — Coursera, Pluralsight, LinkedIn Learning, O'Reilly
- **Meetup** organizers
- **Eventbrite** workshop hosts
- **GitHub** educational repos

Plus auto-fired **domain-specific queries**:
- Salesforce → Trailblazer Ranger search
- Data / ML / AI → HuggingFace + Kaggle
- SAP → SAP Community + SAP Press

### Classifier

- **LLM-based** (Haiku, batched 15 profiles/call, concurrency 2 + global cap)
- 9-class taxonomy: `TRAINER_EXPLICIT`, `FREELANCE_TRAINER`, `TRAINER_IMPLIED`,
  `INSTITUTE`, `PRACTITIONER`, `SPEAKER_ONLY`, `BIG_FIRM`, `NON_INDIA`,
  `MUSTNOT_HIT`, `NOISE`
- Falls back to regex on per-batch failure or LLM unavailability
- Default 16-firm exclusion + persistent-list + client + principal + custom

### Scoring

- Cross-source bonus (+2 per extra source, max +6)
- Web verification: top 20 accepted candidates get a Google query for
  `"<name>" <skill> (workshop OR training OR course)`. **+5 boost** on 2+
  hits, **-3 demote** on 0 hits
- Multi-pass Sonnet rerank on top 30 → top N (`qualityCap` default 15)
- Pre-flight defensibility check: Haiku samples rank 1/mid/last and asks
  "would you send these to a client?" — sets `lowYield` flag if <2/3 pass

### Output

- **Excel** with 14 columns (incl. `Web verify` column)
- Persisted to Postgres (`brief_outputs` table) — survives dyno restarts
- 3 sheets: Candidates / Rejected (audit) / Brief context

### Resilience

- Retry-with-backoff on every external call (Apify, harvestapi, Claude)
- **Watchdog** auto-fails briefs stuck in non-terminal status > 25 min
- **Boot reaper** marks any non-terminal brief as `failed` at server
  startup (orphans from dyno restarts get cleaned up immediately)
- Postgres reconciliation tick retries failed writes every 5 min
- Low-yield warning surfaces in UI when `<3` candidates OR preflight failed
- Global CLI semaphore (cap 2) prevents OOM on concurrent briefs

### Memory across briefs

- **Persistent exclusions** in admin tab (always-skip companies, no
  per-brief retype). Auto-merged into every brief's exclusions list.

## API ENDPOINTS

Auth removed (open URL, internal-use only).

```
GET    /healthz                          Extended: version, uptime, dirty, cliQueue, llm, storage
GET    /api/briefs                       List
POST   /api/briefs                       Create + run pipeline
GET    /api/briefs/:id                   Detail
POST   /api/briefs/:id/retry             Re-run pipeline
POST   /api/briefs/:id/preview           1-query 5-sample preview
POST   /api/briefs/:id/feedback          Form 1 — re-run with feedback steering
POST   /api/briefs/parse                 Free-text intake → structured fields
GET    /api/briefs/:id/output            Download Excel (Postgres-first, FS fallback)
DELETE /api/briefs/:id                   Delete brief + Excel (refuses if running, ?force=1 to override)
GET    /api/feature-requests             Form 2 admin queue list
POST   /api/feature-requests             Form 2 — submit
PATCH  /api/feature-requests/:id         Admin status update
GET    /api/persistent-exclusions        Always-skip list
POST   /api/persistent-exclusions        Add to always-skip list
DELETE /api/persistent-exclusions/:id    Remove from always-skip list
```

## FRONTEND UX (v3.3 additions)

- Status pill in topbar (warm / warming / cold / checking)
- Manual **⚡ Warm up** button + tooltip
- Cold-start banner appears only when needed
- Wizard step 4 shows yellow hint if backend isn't warm at submit
- Detail view: "Now: <latest log>" card during pipeline, low-yield banner
  on flagged runs, Web verify column in download
- Admin tab: Persistent exclusions panel + Feature requests
- Detail view: Danger zone with Delete button

## CRITICAL ENV VARS

Set in Render dashboard. The first 4 are required; the rest have defaults.

```
APIFY_TOKEN                    Required
CLAUDE_CODE_OAUTH_TOKEN        Required (mint via `claude setup-token`)
DATABASE_URL                   Required (Render auto-fills internal URL)
ANTHROPIC_VIA_CLAUDE_CLI=true  Required for Max-plan path

ALLOWED_ORIGINS=*              Default
NODE_VERSION=20                Default
MAX_CONCURRENT_CLI=2           Global CLI subprocess cap
LLM_CLASSIFIER_CONCURRENCY=2   Per-brief classifier batch parallelism
LLM_CLASSIFIER_BATCH_SIZE=15   Profiles per Haiku batch
LINKEDIN_ENRICH_CAP=60         harvestapi cap per brief
WEB_VERIFY_TOP_N=20            Top accepted to web-verify
WEB_VERIFY_CONCURRENCY=3       Parallel verify queries
RERANK_TOP_N=30                Sent to Sonnet for rerank
LOW_YIELD_THRESHOLD=3          Below = lowYield flag
BRIEF_STUCK_TIMEOUT_MS=1500000 25 min watchdog threshold
WATCHDOG_INTERVAL_MS=60000     1 min watchdog scan
RECONCILE_INTERVAL_MS=300000   5 min Postgres dirty-write retry
CLAUDE_CLI_TIMEOUT_MS=180000   CLI subprocess hard timeout
QUALITY_CAP=15                 Final candidate count cap
MAX_QUERIES_STD=12             Std-mode query budget
MAX_QUERIES_NICHE=20           Niche-mode query budget
```

Set `DISABLE_LLM_CLASSIFIER=1`, `DISABLE_WEB_VERIFY=1`, `DISABLE_RERANK=1`,
or `DISABLE_PREFLIGHT_CHECK=1` to opt out of individual quality stages.

## KNOWN ISSUES / WATCH ITEMS

1. **Render Postgres free tier expires 2026-05-31.** Either upgrade Render
   plan (~$7/mo) or migrate to Supabase/Neon free tier before then.
2. **Pre-fix briefs (older than 1e727cf, 2026-05-01) don't have Excel
   persisted to Postgres.** Their `.xlsx` files were lost in earlier dyno
   restarts. New briefs going forward persist correctly. Re-run any old
   brief if you need its Excel.
3. **No rate limiting on public endpoints.** Auth is removed, URL is
   internal-only per operator confirmation.
4. **Anthropic API can have transient 5xx**; affects rerank/preflight
   quality but never breaks the brief (graceful fallback works, observed
   multiple times in stress test).
5. **Niche briefs with thin trainer pools** may legitimately produce
   `lowYield` results — not a bug, intentional quality signal.

## TESTED + VERIFIED ON 2026-05-01 / 2026-05-02

- All 14 v3.2/v3.3 commits deployed via the new GitHub Actions flow
- `/healthz` returns v3.2 + uptime/dirty/cliQueue/llm/storage fields
- Persistent-exclusions CRUD (POST/GET/DELETE) all 200
- Free-text parser correctly extracts brief intent
- **Stress test**: 3 concurrent briefs → all complete in 11 min wall time,
  `cliQueue` stayed ≤ 2 active, no stuck state, no orphans
- **Real RJP zoho-one briefs** (previously stuck for 30+ min) retried
  under the new semaphore — both completed in 13 min and 18.5 min
- Excel downloads work for all post-fix briefs (25 KB files, all 14
  columns including new `Web verify`)
- Boot reaper auto-cleaned the orphaned briefs on Phase 19 deploy
- Frontend: warm-up button, status pill, cold-start banner, low-yield
  banner, danger zone delete button all working in production

## TESTING TOOLS RECOMMENDED

**Free / set-and-forget**:
- UptimeRobot free tier — 1-min uptime checks on `/healthz` + frontend
- `curl`-based smoke test bash script (healthz + briefs + persistent-exclusions)
- Stress test bash script (3 concurrent briefs, watch `cliQueue`)
- k6 free for load testing
- Playwright for browser/UI regression
- GitHub Actions for synthetic e2e cron (not yet wired — operator deferred)

**Paid (only if scaling)**:
- Sentry $26/mo — error monitoring
- Datadog $15/host/mo — APM (overkill for current volume)

## RECENT COMMITS (v3.3 release)

```
7a3dda5  feat(sources): 6 new toggles + refined blogs + domain-specific (v3.3)
71a5894  fix(reliability): global CLI semaphore + boot reaper + 180s timeout (v3.2)
47f1048  feat(perf+admin): parallel classifier (concurrency=3) + DELETE brief + Danger Zone
1e727cf  fix(persistence): Excel bytes in Postgres (survives dyno restarts)
1d4b0be  docs(.env.example): v3.2 knobs documented
8ff7e48  feat(memory): persistent exclusions (admin-managed)
312bd4c  feat(quality): domain-specific prompts + pre-flight defensibility check
4e550a0  feat(queries): L4 YouTube + Udemy + interleaved query order
3df3b5e  feat(rerank): multi-pass Sonnet holistic rerank on top 30
b354490  feat(scoring): merge-by-source + multi-source bonus
8a1c2b9  feat(verify): web-verification of top accepted candidates
f0ca0af  feat(classifier): batched LLM signal classifier (biggest single quality lift)
770e84b  feat(frontend): cold-start banner + warm-up + status pill + now-doing + danger zone
e3a272a  feat(resilience): watchdog + Postgres reconciliation + low-yield warning
553ba9b  feat(pipeline): retry-with-backoff on Apify/harvestapi/Claude
6367867  feat(ci): GitHub Actions for Render deploy + keep-warm cron
```

## WHAT'S NOT YET DONE / MAY COME UP

- **Migrate Postgres before 2026-05-31** (4 weeks from today)
- Optional: **weekly synthetic e2e cron** (~30 min work, ~$0.40/mo) —
  would catch regressions before RJP notices. Operator said "later"
- **Niche brief tuning** if Vijay/Ramesh report quality issues
- **Apollo.io / Lusha contact-enrichment** integration if RJP wants
  automated outreach (not currently a need)
- **Apify dedicated scrapers** (Meetup Scraper, Eventbrite Scraper) —
  currently using Google `site:` filters; could upgrade for richer data

---

## HOW TO START WORK (in a new chat)

1. Don't touch anything yet. Read this README, then read these to reload context:
   - `server/pipeline.js` (the engine, ~1200 lines)
   - `server/server.js` (routes, boot, healthz)
   - `server/store.js` (Postgres + in-memory cache + brief_outputs)
   - `server/anthropic-claude-cli.js` (CLI subprocess + global semaphore)
   - `frontend/index.html` (single-file SPA, ~1800 lines)

2. Confirm health:
   ```bash
   curl -s https://rjp-trainer-intake-server.onrender.com/healthz
   curl -sI https://rjp-trainer-intake.vercel.app/ | head -1
   ```

3. List current briefs:
   ```bash
   curl -s https://rjp-trainer-intake-server.onrender.com/api/briefs | python3 -m json.tool
   ```

4. Then ask the operator what to do.

**Do not** make code changes, redeploy, or burn Apify/Max budget without
explicit go-ahead. Verify everything is green first, then wait.

---

## LOCAL DEV LOOP

```bash
cd server
cp .env.example .env
# fill in APIFY_TOKEN, CLAUDE_CODE_OAUTH_TOKEN
# DATABASE_URL is optional locally — falls back to filesystem JSON
npm install
npm start
```

Frontend hits the local server if you set `API_BASE = 'http://localhost:3000'`
in `frontend/index.html`. Use `MOCK_APIFY=1` to avoid burning Apify credits
during local development.

## REPO LAYOUT

```
client-portal/
├── .github/workflows/
│   ├── render-deploy.yml      Triggers + verifies Render deploy
│   └── keep-warm.yml          Cron pings /healthz every 10 min
├── frontend/
│   └── index.html             Single-file SPA
├── server/
│   ├── server.js              Express app, /api routes, /healthz
│   ├── auth.js                Auth shim (open access, kept for route-wiring compat)
│   ├── pipeline.js            Apify + Claude orchestration + classifier + scoring + Excel
│   ├── store.js               Postgres + in-memory cache + brief_outputs + reconciliation
│   ├── anthropic-claude-cli.js Claude CLI subprocess wrapper + global semaphore
│   ├── package.json
│   ├── render.yaml            Render IaC
│   ├── seed-briefs.json       Seed data (Run01-05 backfill)
│   └── .env.example           Env-var template (incl. v3.2/v3.3 knobs)
└── README.md                  This file
```
