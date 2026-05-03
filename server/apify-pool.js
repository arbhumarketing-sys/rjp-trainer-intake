'use strict';

/**
 * apify-pool.js — multi-account Apify token pool with balance-aware rotation.
 *
 * Why this exists: Apify free tier caps at $5/month per account. RJP runs ~30-50
 * briefs/month at ~$0.30-1.00 each, easily exceeding a single account's free cap.
 * Instead of upgrading to a paid plan, we operate 3 free accounts (study,
 * training, rakhi) plus the original APIFY_TOKEN for $20+ total free budget.
 *
 * The pool picks whichever account has the MOST available budget for each new
 * brief, refreshes spend data every 5 minutes via /users/me/limits, and skips
 * accounts that are at or near their cap (10 cent safety margin to avoid
 * overshooting on the last call).
 *
 * Failover model: per-brief, not per-query. The brief gets one account at start
 * and uses it for all Phase 2 + LinkedIn calls. If THAT account exhausts mid-
 * brief, individual Apify calls return errors and the existing withRetry +
 * graceful-degrade logic kicks in (output thins to Phase 1 + 3a/c only). For
 * the next brief, the pool re-picks based on fresh balances.
 *
 * Env vars (any subset; pool uses what's present):
 *   APIFY_TOKEN          — original / arbhumarketing-sys (legacy slot)
 *   APIFY_TOKEN_STUDY    — study@rjpinfotek.com
 *   APIFY_TOKEN_TRAINING — training@rjpinfotek.com
 *   APIFY_TOKEN_RAKHI    — rakhi@rjpinfotek.com
 *   APIFY_POOL_REFRESH_MS — refresh TTL (default 300_000 = 5 min)
 *   APIFY_POOL_SAFETY_USD — safety margin under cap (default 0.10)
 */

const { ApifyClient } = require('apify-client');

const REFRESH_MS  = parseInt(process.env.APIFY_POOL_REFRESH_MS  || '300000', 10);
const SAFETY_USD  = parseFloat(process.env.APIFY_POOL_SAFETY_USD || '0.10');
const APIFY_LIMITS_URL = 'https://api.apify.com/v2/users/me/limits';

class ApifyPool {
  constructor() {
    this.accounts = [];
    const candidates = [
      { name: 'default',  token: process.env.APIFY_TOKEN          },
      { name: 'study',    token: process.env.APIFY_TOKEN_STUDY    },
      { name: 'training', token: process.env.APIFY_TOKEN_TRAINING },
      { name: 'rakhi',    token: process.env.APIFY_TOKEN_RAKHI    },
    ];
    for (const c of candidates) {
      if (c.token && c.token.trim()) {
        this.accounts.push({
          name: c.name,
          token: c.token.trim(),
          cap: null, spend: null, available: null,
          cycleEnd: null, lastChecked: 0, error: null,
        });
      }
    }
  }

  hasAccounts() { return this.accounts.length > 0; }

  async _refreshOne(acc) {
    try {
      const resp = await fetch(APIFY_LIMITS_URL, {
        headers: { 'Authorization': 'Bearer ' + acc.token },
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const j = await resp.json();
      const d = j && j.data;
      if (!d) throw new Error('no data');
      acc.cap         = d.limits.maxMonthlyUsageUsd;
      acc.spend       = d.current.monthlyUsageUsd;
      acc.available   = Math.max(0, acc.cap - acc.spend);
      acc.cycleEnd    = d.monthlyUsageCycle.endAt;
      acc.lastChecked = Date.now();
      acc.error       = null;
    } catch (e) {
      acc.error       = (e && e.message) || String(e);
      acc.lastChecked = Date.now();
    }
  }

  async refresh(force) {
    const stale = (acc) => force || !acc.lastChecked || (Date.now() - acc.lastChecked) > REFRESH_MS;
    await Promise.all(this.accounts.filter(stale).map(acc => this._refreshOne(acc)));
  }

  /** Returns { name, client, available } for the account with most headroom,
   *  or null if all accounts are at cap or unreachable. */
  async pickAccount() {
    if (!this.accounts.length) return null;
    await this.refresh();
    const eligible = this.accounts.filter(a => a.available != null && a.available > SAFETY_USD);
    if (!eligible.length) return null;
    eligible.sort((a, b) => b.available - a.available);
    const acc = eligible[0];
    return {
      name: acc.name,
      client: new ApifyClient({ token: acc.token }),
      available: acc.available,
    };
  }

  /** Snapshot for healthz. Triggers refresh in the background; returns whatever
   *  is currently cached (so healthz never blocks on Apify reachability). */
  status() {
    // Background refresh, fire-and-forget
    this.refresh().catch(() => {});
    return {
      accounts: this.accounts.map(a => ({
        name:        a.name,
        cap:         a.cap,
        spend:       a.spend == null ? null : Math.round(a.spend * 100) / 100,
        available:   a.available == null ? null : Math.round(a.available * 100) / 100,
        percentUsed: (a.cap && a.spend != null) ? Math.round(a.spend / a.cap * 1000) / 10 : null,
        cycleEnd:    a.cycleEnd,
        lastChecked: a.lastChecked || null,
        error:       a.error,
      })),
      totalCap:       this.accounts.reduce((s, a) => s + (a.cap || 0), 0),
      totalSpend:     Math.round(this.accounts.reduce((s, a) => s + (a.spend || 0), 0) * 100) / 100,
      totalAvailable: Math.round(this.accounts.reduce((s, a) => s + (a.available || 0), 0) * 100) / 100,
      accountCount:   this.accounts.length,
      eligibleCount:  this.accounts.filter(a => a.available != null && a.available > SAFETY_USD).length,
    };
  }
}

const pool = new ApifyPool();

module.exports = { pool, ApifyPool };
