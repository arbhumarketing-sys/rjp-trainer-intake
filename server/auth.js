'use strict';

// Auth removed 2026-05-01 per operator request — open access.
// `login` and `requireAuth` kept as named exports so existing route wiring
// in server.js (auth.login, auth.requireAuth) continues to work without edits.
// Re-introduce real auth by reverting this file (git history has the JWT impl).

function login() {
  return {
    token: 'open',
    user: { name: 'RJP Infotek team', team: 'rjp-infotek' },
  };
}

function requireAuth(req, res, next) {
  req.user = { team: 'rjp-infotek' };
  next();
}

module.exports = { login, requireAuth };
