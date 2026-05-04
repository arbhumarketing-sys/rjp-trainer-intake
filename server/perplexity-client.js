'use strict';

/**
 * perplexity-client.js — minimal Sonar Pro client for the Phase 1.2 (L1.2)
 * live-web named-trainer probe. OpenAI-compatible chat-completions endpoint.
 *
 * Why this exists separately from the Claude path: Perplexity is a distinct
 * billing stream (their own API console) and a different signal (live web
 * search vs Claude's training-data prior). Phase 1 (Claude) gives the curated
 * baseline; Phase 1.2 (Perplexity) adds anyone who became active after the
 * Claude knowledge cutoff or whose footprint is on platforms Claude indexes
 * weakly (UrbanPro listings, Substack, recent YouTube uploads).
 *
 * No-op when PERPLEXITY_API_KEY is unset — the Phase 1.2 block in pipeline.js
 * gates on hasPerplexity() before doing any work, so this scaffold can ship
 * before the operator has a key. When the key arrives: drop it on Render's
 * Environment tab as PERPLEXITY_API_KEY, redeploy (or just restart the dyno
 * — env vars are read live), and Phase 1.2 starts firing on the next brief.
 *
 * Cost guideline: ~$0.015 per Sonar Pro call → ~$0.09-0.15 per brief at the
 * 6-10 keyword cap. Tracked via the usage block returned in each response.
 */

const PERPLEXITY_API_BASE   = process.env.PERPLEXITY_API_BASE   || 'https://api.perplexity.ai';
const PERPLEXITY_TIMEOUT_MS = parseInt(process.env.PERPLEXITY_TIMEOUT_MS || '60000', 10);
const PERPLEXITY_MODEL      = process.env.PERPLEXITY_MODEL      || 'sonar-pro';

function hasPerplexity() {
  if (process.env.DISABLE_PERPLEXITY === '1') return false;
  return !!process.env.PERPLEXITY_API_KEY;
}

/**
 * Single chat-completion call. Throws with .status mimicking the SDK
 * convention so withRetry's isRetryable() classifies failures correctly.
 *   401 → terminal (bad key, don't retry)
 *   408/429/5xx → transient (retry)
 *   504 → timeout (retry)
 */
async function perplexityChat({ system, user, maxTokens, model, timeoutMs } = {}) {
  const apiKey = process.env.PERPLEXITY_API_KEY;
  if (!apiKey) {
    const e = new Error('Live web probe not configured');
    e.status = 401;
    throw e;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs || PERPLEXITY_TIMEOUT_MS);

  try {
    const resp = await fetch(`${PERPLEXITY_API_BASE}/chat/completions`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type':  'application/json',
      },
      body: JSON.stringify({
        model:       model || PERPLEXITY_MODEL,
        max_tokens:  maxTokens || 1500,
        messages: [
          { role: 'system', content: system || '' },
          { role: 'user',   content: user   || '' },
        ],
      }),
      signal: controller.signal,
    });

    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      const e = new Error(`Live web probe ${resp.status}: ${body.slice(0, 300)}`);
      e.status = resp.status;
      throw e;
    }

    const data = await resp.json();
    const text = (data && data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content) || '';
    const citations = Array.isArray(data && data.citations) ? data.citations : [];
    return { text, citations, usage: (data && data.usage) || null, model: (data && data.model) || PERPLEXITY_MODEL };
  } catch (e) {
    if (e.name === 'AbortError') {
      const err = new Error(`Live web probe timed out after ${timeoutMs || PERPLEXITY_TIMEOUT_MS}ms`);
      err.status = 504;
      throw err;
    }
    if (!e.status) e.status = 500;
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

module.exports = {
  perplexityChat,
  hasPerplexity,
  _PERPLEXITY_MODEL: PERPLEXITY_MODEL,
};
