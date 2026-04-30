/**
 * Shared-password auth + JWT issuance.
 *
 * MVP: single SHARED_PASSWORD env var. The whole RJP Infotek team uses it.
 * Phase 2: per-user accounts with bcrypt + Supabase Auth or Clerk.
 */
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

const SHARED_PASSWORD = process.env.SHARED_PASSWORD || '';
const JWT_SECRET = process.env.JWT_SECRET || '';
const TOKEN_TTL_HOURS = 24 * 14;  // 14 days

if (!SHARED_PASSWORD || !JWT_SECRET) {
  console.warn('[auth] SHARED_PASSWORD or JWT_SECRET not set — login will fail.');
}

function timingSafeEqual(a, b) {
  const A = Buffer.from(a, 'utf8');
  const B = Buffer.from(b, 'utf8');
  if (A.length !== B.length) return false;
  return crypto.timingSafeEqual(A, B);
}

function login(password) {
  if (!SHARED_PASSWORD || !JWT_SECRET) return null;
  if (!timingSafeEqual(password || '', SHARED_PASSWORD)) return null;
  const token = jwt.sign(
    { team: 'rjp-infotek' },
    JWT_SECRET,
    { expiresIn: TOKEN_TTL_HOURS + 'h' }
  );
  return {
    token,
    user: { name: 'RJP Infotek team', team: 'rjp-infotek' },
  };
}

function requireAuth(req, res, next) {
  // Support Bearer header OR ?token=... (used for download links)
  const auth = req.headers.authorization || '';
  let token = '';
  if (auth.startsWith('Bearer ')) token = auth.slice(7);
  else if (req.query && req.query.token) token = String(req.query.token);
  if (!token) return res.status(401).send('Missing token');
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch (e) {
    return res.status(401).send('Invalid token');
  }
}

module.exports = { login, requireAuth };
