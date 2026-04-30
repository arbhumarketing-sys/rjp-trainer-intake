# RJP Infotek — Trainer Sourcing Portal

A two-piece system: a static frontend the RJP team logs into, and a Render-hosted server that runs the sourcing pipeline.

```
client-portal/
├── frontend/           Static site — deploy on Netlify
│   └── index.html      Single-file app (login → wizard → progress → download)
├── server/             Node service — deploy on Render
│   ├── server.js       Express app, /api routes
│   ├── auth.js         Shared-password + JWT
│   ├── pipeline.js     Apify orchestration + scoring + xlsx build
│   ├── store.js        File-based brief store (data/*.json)
│   ├── package.json
│   ├── render.yaml     Render IaC
│   └── .env.example    Env-var template
└── README.md           This file
```

## How it works

1. RJP team member visits the Netlify URL → enters shared password → JWT issued (14-day TTL).
2. They click **+ New brief** → 4-step wizard (operator, domain & roles, location & specifics, review).
3. Submitting POSTs to the Render server. Server writes the brief to disk, kicks off the pipeline as a background job, and returns immediately.
4. Frontend polls `GET /api/briefs/:id` every 2.5s. Server runs Apify queries → dedupe → score → build `.xlsx`. Each step updates `status` + `log` on the brief.
5. When `status === 'complete'`, the frontend shows a **Download** button. The download URL hits `/api/briefs/:id/output?token=...` (token in query so the link works as a plain `<a download>`).
6. Repository on the left shows every past brief, status pill, click to re-open the detail view and re-download.

The pipeline is fire-and-forget on the server — your laptop being off has no effect.

## What's MVP vs Phase 2

**Shipped**
- Login, brief wizard, repository, live status polling, download
- Apify `rag-web-browser` actor integration
- Provisional auto-scoring against the v2 rubric (signal/bucketFit/verify/book)
- `.xlsx` output with two tabs: ranked candidates + brief context (incl. steering text)

**Stubbed (works, but conservative output)**
- Auto-classification of signal: keyword-based heuristic. Searcher should tighten before client delivery.
- Geo: keyword match on India city names. Tighten with structured location extraction in v2.

**Phase 2 (logged in project-context.md, not built)**
- Per-client logins (Supabase Auth or Clerk)
- Google Sheet output (currently buttons exist but pipeline produces .xlsx — Sheet upload via Google Drive API is a clean follow-up)
- PDF output
- Clay enrichment middle layer (waterfall: email, phone, certifications, Credly badges)
- LinkedIn Sales Nav + harvestapi for richer profile fields
- Real-time stage updates via SSE rather than polling

## Deploy — server (Render)

You'll need an Apify token and a strong password.

1. Push this `server/` folder to a Git repo. The cleanest pattern is its own repo: `gh repo create rjp-sourcing-server --private --source=server --push`.
2. In Render: **New → Web Service → Connect repo → pick the new repo**.
3. Render auto-detects `package.json`. If it doesn't pick up `render.yaml`, set:
   - Runtime: Node
   - Build command: `npm install`
   - Start command: `node server.js`
   - Health check path: `/healthz`
4. **Set env vars** (Settings → Environment):
   - `APIFY_TOKEN` — from https://console.apify.com/settings/integrations
   - `SHARED_PASSWORD` — pick a strong one. The whole RJP team uses this.
   - `JWT_SECRET` — `openssl rand -hex 32` or let Render generate it via `render.yaml`
   - `ALLOWED_ORIGINS` — `https://your-netlify-site.netlify.app` (update after step 6)
5. Add a **persistent disk** (Render → Settings → Disks): mount path `/data`, 1 GB. Then add env var `DATA_DIR=/data/briefs` and `OUTPUT_DIR=/data/outputs`. Without a disk, briefs disappear on every redeploy.
6. Deploy. Note the Render URL — looks like `https://rjp-sourcing-portal.onrender.com`.

## Deploy — frontend (Netlify)

1. Open `frontend/index.html` and edit the top of the script:
   ```js
   const API_BASE = 'https://rjp-sourcing-portal.onrender.com';
   ```
2. Drag the `frontend/` folder onto https://app.netlify.com/drop.
3. Netlify gives you a URL. Copy it.
4. Go back to Render → Settings → Environment → set `ALLOWED_ORIGINS` to that Netlify URL → redeploy.
5. Open the Netlify URL → log in with `SHARED_PASSWORD` → submit a test brief.

## Local test loop

```bash
cd server
cp .env.example .env
# fill in APIFY_TOKEN, SHARED_PASSWORD, JWT_SECRET
npm install
npm start
# in another shell, serve frontend (or just open the file in a browser, since API_BASE='' uses same origin)
# but for full local dev:
cd ../frontend
python3 -m http.server 5500
```

Edit `frontend/index.html` → set `API_BASE = 'http://localhost:3000'` for local testing. Set it back to the Render URL before deploying.

## What the team sees

**Login** — RJ logo, shared-password field. One screen, one click.

**Repository** (sidebar) — past briefs with status pills (`queued / running / scoring / building / complete / failed`).

**+ New brief** wizard:
1. Operator name + email
2. Domain dropdown + roles list (one row per role: title / skill / bucket / count)
3. Location chips + Exclusions textarea + **Specific direction** free-text + Output format
4. Review + submit

**Brief detail** — meta strip (domain, location, output, headcount, status), 5-stage progress board with live updates and a log stream below it, then a green download bar when the file is ready.

## Costs

- Render Starter web service: free for 750 hours/month, $7/month for always-on.
- Render persistent disk 1 GB: $0.25/month.
- Netlify static hosting: free.
- Apify credits: paid plan you already have. Each brief uses ~12 actor runs × 20 results = ~240 page fetches per brief. Budget ~₹40-80 per brief on rag-web-browser.
- Total per-brief cost: rounding to ₹100 including Render allocation.

## Failure modes & recovery

- **Apify token missing or wrong** → pipeline marks brief `failed`, log shows the error. Fix token in Render env vars, hit Retry on the brief detail page.
- **Apify rate-limited** → some queries fail, brief still completes with whatever was discovered. Re-submit if needed.
- **Render free tier sleeps** → first brief after idle takes ~30s to wake the server. Subsequent briefs are fast. Upgrade to $7 always-on if this is annoying.
- **Disk full** → briefs older than 90 days can be archived/deleted by hand, or wire a cleanup cron. Phase 2.

## Not exposed to clients yet

- Per-team file isolation (everyone with the password sees everyone's briefs)
- Audit trail of who submitted what (operator name is captured but not enforced)
- Pre-baked deletion / archive UI
