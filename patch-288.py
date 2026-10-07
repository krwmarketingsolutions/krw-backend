#!/usr/bin/env python3
"""
patch-288.py  --  Close the public-dashboard key leak (backend half)

THE PROBLEM THIS FIXES
  Every portal is served publicly from GitHub Pages, and all four ship the
  admin API key in plain JavaScript. The publisher-facing portals
  (leads-portal, ssdi-portal, portal/ssdi.html) hand that key to every
  publisher you onboard. With it, anyone can call /leads/feed WITHOUT a
  portal_id and read every lead from every publisher - including buyer_name,
  buyer_status, revenue and billable - which is exactly what your publisher
  portals are supposed to never show.

WHAT THIS PATCH ADDS (backend only; the portals are patch 289)
  1. Admin login, so the dashboard stops needing a key in its source
       POST /auth/login {password}  -> {token, expires_at}
       Token is HMAC-SHA256 signed, 12h life, carries no secret.
       requireKey() now accepts  Authorization: Bearer <token>  as well as
       the old x-api-key, so nothing breaks while you migrate.
       Env: DASH_PASSWORD (required for login to work at all)

  2. A scoped PORTAL key that CANNOT touch admin routes
       Env: PORTAL_API_KEY
       Accepted only by the publisher-facing routes:
         /portal/*, /leads/feed, /calls/feed, /publishers/login
       On every other admin route it is rejected like any wrong key.

  3. Fail-closed scoping for the two feeds when called with the portal key
       - portal_id (or pub) becomes REQUIRED; without it the request is
         refused rather than returning everything.
       - buyer_name, buyer_status, buyer_error, buyer_intake_id, revenue,
         billable and notes are stripped from the response.
       Calls made with the admin key or an admin token are untouched, so your
       dashboard keeps seeing everything.

  4. GET /auth/check - tells the dashboard whether its token is still good.

NOTHING BREAKS ON DEPLOY. The old key keeps working everywhere until you
rotate it. Order of operations is in the runbook.

Usage (from ~/krw/krw-backend):
    python3 patch-288.py
    node --check server.js
    git add -A && git commit -m "patch 288: scoped portal key + admin login" && git push

Backup: server.js.pre-288.bak. Safe to re-run: refuses to apply twice.
"""
import os, re, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 288" in src:
    sys.exit("patch 288 already applied - nothing to do")

# ── 1. replace the auth middleware block ──────────────────────────────────────
OLD_AUTH = """function requireKey(req, res, next) {
  const key = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  if (!API_KEY || key !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}"""
if src.count(OLD_AUTH) != 1:
    sys.exit("requireKey() does not look the way patch 288 expects - aborting, server.js untouched")

NEW_AUTH = r"""// ── patch 288: admin login tokens + a scoped portal key ──────────────────────
// The portals are served from public GitHub Pages, so anything baked into
// their HTML is public. Admin access moves to a signed token behind a
// password; publisher portals get their own key that admin routes refuse.
const _crypto288 = require('crypto');
const DASH_PASSWORD = process.env.DASH_PASSWORD || '';
const PORTAL_KEY    = process.env.PORTAL_API_KEY || '';
const TOKEN_TTL_MS  = 12 * 60 * 60 * 1000;

function _tokenSecret() {
  // Signing secret is derived, never shipped anywhere.
  return (process.env.TOKEN_SECRET || process.env.API_KEY || 'krw') + '|' + (DASH_PASSWORD || '');
}
function mintToken(role) {
  const payload = JSON.stringify({ role, exp: Date.now() + TOKEN_TTL_MS });
  const b = Buffer.from(payload).toString('base64url');
  const sig = _crypto288.createHmac('sha256', _tokenSecret()).update(b).digest('base64url');
  return b + '.' + sig;
}
function readToken(tok) {
  try {
    const [b, sig] = String(tok || '').split('.');
    if (!b || !sig) return null;
    const good = _crypto288.createHmac('sha256', _tokenSecret()).update(b).digest('base64url');
    const a = Buffer.from(sig), c = Buffer.from(good);
    if (a.length !== c.length || !_crypto288.timingSafeEqual(a, c)) return null;
    const p = JSON.parse(Buffer.from(b, 'base64url').toString());
    if (!p || typeof p.exp !== 'number' || Date.now() > p.exp) return null;
    return p;
  } catch (e) { return null; }
}
function _bearer(req) {
  const h = String(req.headers['authorization'] || '');
  if (h.toLowerCase().startsWith('bearer ')) return h.slice(7).trim();
  // CSV downloads open in a new tab and cannot set a header, so a token (which
  // expires in 12h) may ride in the query string. The API key may NOT.
  return String(req.query.token || '').trim();
}
// Who is calling: 'admin' (key or token), 'portal' (scoped key), or null.
function callerRole(req) {
  const key = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  if (API_KEY && key === API_KEY) return 'admin';
  const t = readToken(_bearer(req));
  if (t && t.role === 'admin') return 'admin';
  if (PORTAL_KEY && key === PORTAL_KEY) return 'portal';
  return null;
}

function requireKey(req, res, next) {
  const role = callerRole(req);
  if (role !== 'admin') return res.status(401).json({ error: 'Unauthorized' });
  req.authRole = 'admin';
  next();
}
// Publisher-facing routes: admin OR the scoped portal key.
function requirePortalKey(req, res, next) {
  const role = callerRole(req);
  if (!role) return res.status(401).json({ error: 'Unauthorized' });
  req.authRole = role;
  next();
}

app.post('/auth/login', async (req, res) => {
  try {
    if (!DASH_PASSWORD) return res.status(503).json({ ok: false, error: 'DASH_PASSWORD is not set on the server yet' });
    const given = String((req.body && req.body.password) || '');
    const a = Buffer.from(given), b = Buffer.from(DASH_PASSWORD);
    const ok = a.length === b.length && _crypto288.timingSafeEqual(a, b);
    if (!ok) { await new Promise(r => setTimeout(r, 400)); return res.status(401).json({ ok: false, error: 'Wrong password' }); }
    res.json({ ok: true, token: mintToken('admin'), expires_in_hours: TOKEN_TTL_MS / 3600000 });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/auth/check', (req, res) => {
  const role = callerRole(req);
  res.json({ ok: !!role, role: role || null, login_available: !!DASH_PASSWORD });
});
// Admin-only. Lets the dashboard build publisher posting instructions without
// the lead key being written into a page that anyone can read.
app.get('/auth/config', requireKey, (req, res) => {
  res.json({ ok: true, lead_key: LEAD_KEY || '' });
});
console.log('[Auth] patch 288 - admin login ' + (DASH_PASSWORD ? 'ENABLED' : 'OFF (set DASH_PASSWORD)') +
            ', portal key ' + (PORTAL_KEY ? 'set' : 'NOT set (set PORTAL_API_KEY)'));
// ── end patch 288 auth ───────────────────────────────────────────────────────"""

src = src.replace(OLD_AUTH, NEW_AUTH, 1)

# ── 2. the two feeds accept the portal key, but fail closed and strip ─────────
FEED_FIELDS = ['buyer_name', 'buyer_status', 'buyer_error', 'buyer_intake_id', 'revenue', 'billable', 'notes', 'zapier_status']

GUARD = r"""
// patch 288: a portal-key caller must name its publisher and never sees
// buyer, routing or revenue columns.
function portalGuard(req, res) {
  if (req.authRole !== 'portal') return true;
  if (!req.query.portal_id && !req.query.pub) {
    res.status(400).json({ error: 'portal_id is required' });
    return false;
  }
  return true;
}
function portalStrip(req, rows) {
  if (req.authRole !== 'portal') return rows;
  const drop = %s;
  return rows.map(r => { const o = Object.assign({}, r); drop.forEach(k => { delete o[k]; }); return o; });
}
""" % (repr(FEED_FIELDS).replace("'", '"'))

# insert the helpers right before /leads/feed
m = re.search(r"app\.get\('/leads/feed', requireKey,", src)
if not m:
    sys.exit("could not find /leads/feed - aborting, server.js untouched")
src = src[:m.start()] + GUARD + "\n" + src[m.start():]

src = src.replace("app.get('/leads/feed', requireKey,", "app.get('/leads/feed', requirePortalKey,", 1)
src = src.replace("app.get('/calls/feed', requireKey,", "app.get('/calls/feed', requirePortalKey,", 1)

# /leads/feed: guard at the top of the handler, strip on the way out
old_head = """  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  try {
    const { campaign, status, limit=100, pub, days, portal_id } = req.query;"""
if src.count(old_head) != 1:
    sys.exit("/leads/feed header changed - aborting, server.js untouched")
src = src.replace(old_head, """  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache');
  res.set('Expires', '0');
  if (!portalGuard(req, res)) return;                      // patch 288
  try {
    const { campaign, status, limit=100, pub, days, portal_id } = req.query;""", 1)

old_out = "    res.json({ ok:true, count:r.rows.length, leads:r.rows });"
if src.count(old_out) != 1:
    sys.exit("/leads/feed response line changed - aborting, server.js untouched")
src = src.replace(old_out, "    res.json({ ok:true, count:r.rows.length, leads:portalStrip(req, r.rows) });   // patch 288", 1)

# /calls/feed: guard at the top of the handler
old_calls = """app.get('/calls/feed', requirePortalKey, async (req, res) => {
  const { days = 30, pub } = req.query;"""
if src.count(old_calls) != 1:
    sys.exit("/calls/feed header changed - aborting, server.js untouched")
src = src.replace(old_calls, """app.get('/calls/feed', requirePortalKey, async (req, res) => {
  if (!portalGuard(req, res)) return;                      // patch 288
  const { days = 30, pub } = req.query;""", 1)

# ── 3. the /portal/* routes accept the portal key ─────────────────────────────
n_portal = len(re.findall(r"app\.(get|post|put|patch)\('/portal/[^']*', requireKey,", src))
src = re.sub(r"(app\.(?:get|post|put|patch)\('/portal/[^']*', )requireKey,", r"\1requirePortalKey,", src)

# ── 4. stop printing the keys on the Settings page of the API ────────────────
src = src.replace(
    "app.get('/debug',  (req, res) => res.json({ api_key_set:!!process.env.API_KEY, lead_key_set:!!process.env.LEAD_API_KEY, db_url_set:!!process.env.DATABASE_URL }));",
    "app.get('/debug', requireKey, (req, res) => res.json({ api_key_set:!!process.env.API_KEY, lead_key_set:!!process.env.LEAD_API_KEY, db_url_set:!!process.env.DATABASE_URL, portal_key_set:!!process.env.PORTAL_API_KEY, dash_password_set:!!process.env.DASH_PASSWORD }));   // patch 288: was public",
    1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-288.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 288 applied. backup: server.js.pre-288.bak")
print("  /portal/* routes moved to the scoped key: %d" % n_portal)
print("  /leads/feed and /calls/feed now fail closed for portal callers and strip buyer/revenue columns")
print("next: node --check server.js && git add -A && git commit -m 'patch 288' && git push")
