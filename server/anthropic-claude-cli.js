'use strict';

/**
 * anthropic-claude-cli.js — drop-in replacement for `@anthropic-ai/sdk` that
 * routes calls through the local `claude` CLI subprocess (Claude Code) instead
 * of api.anthropic.com.
 *
 * Why this exists: the Anthropic API console and Claude Pro/Max subscriptions
 * are SEPARATE billing streams. A user with a $100 Max plan still has $0 in
 * API credits unless they top up the console. The `claude` CLI authenticates
 * via OAuth (Claude Code login) and bills against the Max subscription —
 * not the API console — so spawning it as a subprocess lets a third-party
 * Node app tap into the user's Max quota.
 *
 * This is documented as UNSUPPORTED by Anthropic for third-party agents (see
 * https://code.claude.com/docs/en/agent-sdk/overview). Enable only when the
 * operator explicitly opts in via `ANTHROPIC_VIA_CLAUDE_CLI=true` in `.env`.
 *
 * Tradeoffs vs direct API:
 *   ~6-9s per call vs ~1s          (subprocess spawn + Claude Code init overhead)
 *   Max-plan rate limits apply      (5-hour rolling window; bursty arbhu flows
 *                                    like buy_leads_purchase with 36 chunks
 *                                    in 2 min may trip caps mid-batch)
 *   No max_tokens enforcement       (CLI uses model default ~32000)
 *   Output may be slightly more verbose than API mode (CLI applies its
 *                                    default Claude Code framing on top of
 *                                    --system-prompt)
 *   No mid-stream / streaming       (we always wait for full subprocess exit)
 *
 * What we mimic from the SDK:
 *   client.messages.create(params) → { id, model, role, content, stop_reason, usage }
 *
 * What we don't (yet):
 *   client.messages.stream()
 *   client.messages.batches.*       (Buy Leads bursts could benefit but the
 *                                    CLI has no equivalent batch primitive)
 *   any non-text content blocks (images, tool_use)
 */

const { spawn } = require('child_process');

const CLI_BIN          = process.env.CLAUDE_CLI_BIN || 'claude';
const CLI_TIMEOUT_MS   = parseInt(process.env.CLAUDE_CLI_TIMEOUT_MS || '180000', 10);

// Global CLI semaphore — caps total concurrent `claude` subprocesses across
// the WHOLE Node process. Each subprocess loads its own Node + Claude Code
// SDK (~50-100 MB resident) and competes with siblings for CPU. On Render
// free tier (512 MB cap, shared CPU), more than 2-3 concurrent CLI calls
// causes OOM-kill / page-swap, which manifests as 120s subprocess timeouts
// and "LLM CLI timed out" warnings in brief logs. This semaphore
// protects against that for both single-brief parallelism (e.g., classifier
// concurrency) AND multi-brief parallelism (RJP submitting 2 briefs while
// 1 is still running).
const MAX_CONCURRENT_CLI = parseInt(process.env.MAX_CONCURRENT_CLI || '2', 10);
let _activeCli = 0;
const _cliQueue = [];

function _acquireCliSlot() {
  return new Promise(resolve => {
    if (_activeCli < MAX_CONCURRENT_CLI) {
      _activeCli++;
      return resolve();
    }
    _cliQueue.push(() => { _activeCli++; resolve(); });
  });
}

function _releaseCliSlot() {
  _activeCli--;
  const next = _cliQueue.shift();
  if (next) next();
}

// Lightweight surface for telemetry / health endpoints
function getCliQueueStats() {
  return { active: _activeCli, waiting: _cliQueue.length, max: MAX_CONCURRENT_CLI };
}

/**
 * Flatten the SDK's `messages: [{role, content}]` array into a single string
 * for the CLI's stdin. Multi-turn conversations get concatenated with
 * "Human:"/"Assistant:" markers so the model sees the prior turns.
 */
function flattenMessages(messages) {
  if (!Array.isArray(messages)) return '';
  const parts = [];
  for (const m of messages) {
    const role = m.role === 'assistant' ? 'Assistant' : 'Human';
    const content = typeof m.content === 'string'
      ? m.content
      : Array.isArray(m.content)
        ? m.content.map(b => b.type === 'text' ? b.text : '').join('\n')
        : String(m.content || '');
    parts.push(`${role}: ${content}`);
  }
  return parts.join('\n\n');
}

/**
 * Spawn `claude --print --output-format json` with the given system + user
 * prompt. Resolves with the raw CLI JSON; rejects with an Error whose .status
 * mimics the SDK's HTTP status convention so existing retry/circuit-breaker
 * logic in message-generator.js works unchanged.
 *
 * Wrapped in a global semaphore (_acquireCliSlot / _releaseCliSlot) so the
 * total number of in-flight `claude` subprocesses across the Node process
 * stays under MAX_CONCURRENT_CLI. The actual subprocess work is delegated
 * to _spawnClaudeCli; this outer function only handles the queueing.
 */
async function callClaudeCli(args) {
  await _acquireCliSlot();
  try {
    return await _spawnClaudeCli(args);
  } finally {
    _releaseCliSlot();
  }
}

function _spawnClaudeCli({ model, system, prompt, timeoutMs }) {
  return new Promise((resolve, reject) => {
    const args = ['--print', '--output-format', 'json', '--model', model];
    if (system) {
      args.push('--system-prompt', system);
    }
    // 2026-04-29: when CLAUDE_CODE_OAUTH_TOKEN is set, scrub ANTHROPIC_API_KEY
    // from the child env. The CLI prefers the API key when both are set, which
    // routes us back to API console credits ($0 balance) instead of the
    // operator's Max subscription. Inheriting parent env minus that one var
    // is enough to force OAuth.
    const childEnv = { ...process.env };
    if (childEnv.CLAUDE_CODE_OAUTH_TOKEN) {
      delete childEnv.ANTHROPIC_API_KEY;
    }
    const proc = spawn(CLI_BIN, args, { stdio: ['pipe', 'pipe', 'pipe'], env: childEnv });
    let stdout = '';
    let stderr = '';
    let settled = false;

    const finish = (err, data) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      try { proc.kill('SIGTERM'); } catch {}
      if (err) reject(err);
      else resolve(data);
    };

    const timer = setTimeout(() => {
      const err = new Error(`LLM CLI timed out after ${timeoutMs}ms`);
      err.status = 504;
      finish(err);
    }, timeoutMs);

    proc.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
    proc.stderr.on('data', (chunk) => { stderr += chunk.toString(); });

    proc.on('error', (err) => {
      err.status = 500;
      finish(err);
    });

    proc.on('close', (code) => {
      if (code !== 0) {
        const err = new Error(`LLM CLI exit ${code}: stderr="${stderr.substring(0, 400)}" stdout="${stdout.substring(0, 400)}"`);
        err.status = 500;
        return finish(err);
      }
      let parsed;
      try {
        parsed = JSON.parse(stdout);
      } catch (e) {
        const err = new Error(`LLM CLI returned non-JSON output: ${stdout.substring(0, 200)}`);
        err.status = 502;
        return finish(err);
      }
      // CLI signals errors via { is_error: true, result: "<msg>" }
      if (parsed.is_error) {
        const msg = parsed.result || 'unknown CLI error';
        const err = new Error(`LLM CLI error: ${msg}`);
        // Login-required → mimic 401 so health-watchdog records auth failure
        err.status = /login|auth|credential|not logged/i.test(msg) ? 401 : 500;
        err.cliResponse = parsed;
        return finish(err);
      }
      finish(null, parsed);
    });

    try {
      proc.stdin.write(prompt);
      proc.stdin.end();
    } catch (e) {
      const err = new Error(`stdin write failed: ${e.message}`);
      err.status = 500;
      finish(err);
    }
  });
}

/**
 * Map CLI JSON → SDK-shaped response object.
 */
function cliToSdkResponse(cliJson, requestedModel) {
  return {
    id:          'msg_' + (cliJson.uuid || cliJson.session_id || Date.now()),
    type:        'message',
    role:        'assistant',
    model:       requestedModel,
    content:     [{ type: 'text', text: cliJson.result || '' }],
    stop_reason: cliJson.stop_reason || 'end_turn',
    stop_sequence: null,
    usage: {
      input_tokens:  (cliJson.usage && cliJson.usage.input_tokens)  || 0,
      output_tokens: (cliJson.usage && cliJson.usage.output_tokens) || 0,
      cache_creation_input_tokens: (cliJson.usage && cliJson.usage.cache_creation_input_tokens) || 0,
      cache_read_input_tokens:     (cliJson.usage && cliJson.usage.cache_read_input_tokens)     || 0,
    },
    // Carry a debug breadcrumb so it shows up in arbhu's logs without
    // breaking SDK shape consumers.
    _viaClaudeCli: {
      duration_ms:    cliJson.duration_ms || cliJson.duration_api_ms,
      total_cost_usd: cliJson.total_cost_usd,
      session_id:     cliJson.session_id,
    },
  };
}

class ClaudeCliClient {
  constructor(opts = {}) {
    this._model         = opts.model || null; // optional default
    this._timeoutMs     = opts.timeoutMs || CLI_TIMEOUT_MS;
    this.messages = {
      create: (params) => this._createMessage(params),
    };
  }

  async _createMessage(params) {
    const model      = params.model || this._model;
    if (!model) {
      const e = new Error('LLM CLI: model is required');
      e.status = 400; throw e;
    }
    const system     = typeof params.system === 'string'
      ? params.system
      : Array.isArray(params.system)
        ? params.system.map(b => b.type === 'text' ? b.text : '').join('\n\n')
        : '';
    const prompt     = flattenMessages(params.messages);
    const cli        = await callClaudeCli({ model, system, prompt, timeoutMs: this._timeoutMs });
    return cliToSdkResponse(cli, model);
  }
}

module.exports = {
  ClaudeCliClient,
  getCliQueueStats,
  // exported for tests
  _flattenMessages: flattenMessages,
  _cliToSdkResponse: cliToSdkResponse,
};
