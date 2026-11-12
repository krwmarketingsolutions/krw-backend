#!/usr/bin/env python3
r"""
patch-388.py  --  partner logins for the Deal Dashboard (Andrew)

Kyler, Nov 12: Andrew (partner) gets his own login to the dashboard. He sees
the lead flow, the lead charts and summaries, the funnel, and the homepage
with the buyer sheets. He does NOT get the billable approval list and he sees
nothing SSDI.

How it works:
  - Logins live in a new dash_users table (name, role, scrypt password hash).
    Kyler creates them from his own portal; the password never touches this
    repo or an env var.
  - /auth/login: the admin password works as before. Any other password is
    checked against the active logins and gets a token with role 'partner'.
  - A partner token is READ ONLY and allow-listed. It only works on:
        GET /auth/check, /leads/feed, /leads/summary, /leads/detail/:id,
            /buyer-sheets/status, /dashboard/funnel
    Everything else (every POST/PUT/DELETE, the billable queue, calls, SSDI,
    publishers, invoices, settings, exports) answers
        200 { ok:false, restricted:true }
    so the dashboard does not treat it as an expired session.
  - On the allowed routes the query is scoped (no campaign / pub / vertical
    overrides, so the SSDI exclusion always applies) and the response is
    filtered: no SSDI rows, no SSDI buyers or funnel section, no pending
    approval counts. If the filter fails the request is refused.
  - Turning a login off or resetting its password ends its sessions.

New admin routes (requireKey):
    GET  /auth/users                 list logins
    POST /auth/users                 { name, password }  create or reset
    POST /auth/users/:id/active      { active: true|false }

Backup: server.js.pre-388.bak    Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None
ROOT = find_root() or sys.exit("run from ~/krw (needs krw-backend/)")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()
if "patch 388" in src:
    sys.exit("patch 388 already applied - nothing to do")

LOGIN_ANCHOR = "app.post('/auth/login', async (req, res) => {"

PARTNER_BLOCK = r'''// ── patch 388: partner logins (read only, allow-listed, no SSDI) ─────────────
const PARTNER_PAGES = ['home', 'leads', 'funnel'];
const PARTNER_GET_ALLOW = [/^\/auth\/check$/, /^\/leads\/feed$/, /^\/leads\/summary$/, /^\/leads\/detail\/\d+$/, /^\/buyer-sheets\/status$/, /^\/dashboard\/funnel$/];
const PARTNER_QUERY_KEEP = ['limit', 'days', 'status', 'period', 'from', 'to'];
const _scrypt388 = require('util').promisify(_crypto288.scrypt);
const _dashUsersReady = pool.query(`CREATE TABLE IF NOT EXISTS dash_users (
    id SERIAL PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'partner',
    pass_salt TEXT NOT NULL, pass_hash TEXT NOT NULL, pass_ver INT NOT NULL DEFAULT 1,
    active BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMPTZ DEFAULT NOW(), last_login_at TIMESTAMPTZ)`)
  .catch(e => { console.error('[Auth] dash_users init failed:', e.message); });
let _dashUsers = { at: 0, byId: {} };
async function dashUsersLoad(force) {
  if (!force && Date.now() - _dashUsers.at < 30000) return _dashUsers.byId;
  await _dashUsersReady;
  const r = await pool.query(`SELECT id, name, role, active, pass_ver FROM dash_users`);
  const m = {}; r.rows.forEach(u => { m[u.id] = u; });
  _dashUsers = { at: Date.now(), byId: m };
  return m;
}
async function dashHash(password, salt) { return (await _scrypt388(String(password), salt, 64)).toString('hex'); }
async function dashUserByPassword(given) {
  if (!given) return null;
  await _dashUsersReady;
  const r = await pool.query(`SELECT id, name, role, pass_salt, pass_hash, pass_ver FROM dash_users WHERE active = true ORDER BY id`);
  let hit = null;
  for (const u of r.rows) {   // check every login so timing does not show which one matched
    const a = Buffer.from(await dashHash(given, u.pass_salt), 'hex'), b = Buffer.from(u.pass_hash, 'hex');
    if (a.length === b.length && _crypto288.timingSafeEqual(a, b) && !hit) hit = u;
  }
  return hit;
}
function partnerSanitize(path, body) {
  if (!body || typeof body !== 'object') return body;
  const isSsdi = (x) => /ssdi/i.test(String((x && (x.vertical || '')) || '')) || /ssdi/i.test(String((x && (x.campaign || '')) || ''));
  if (path === '/leads/feed') {
    if (Array.isArray(body.leads)) { body.leads = body.leads.filter(l => !isSsdi(l)); body.count = body.leads.length; }
    return body;
  }
  if (/^\/leads\/detail\//.test(path)) {
    if (body.lead && isSsdi(body.lead)) return { ok: false, restricted: true, error: 'Not available for this login' };
    return body;
  }
  if (path === '/buyer-sheets/status') {
    if (Array.isArray(body.buyers)) {
      body.buyers = body.buyers.filter(b => !/ssdi/i.test(String(b.vertical || '')))
        .map(b => Object.assign({}, b, { pending_approval: 0, pending_amount: 0 }));
    }
    return body;
  }
  if (path === '/dashboard/funnel') {
    const ssdiPubs = Object.keys((body.ssdi && body.ssdi.publishers) || {});
    delete body.ssdi;
    if (body.routing) Object.keys(body.routing).forEach(k => { if (ssdiPubs.includes(k) || /ssdi/i.test(k)) delete body.routing[k]; });
    return body;
  }
  return body;
}
app.use(async (req, res, next) => {
  let t = null;
  try { t = readToken(_bearer(req)); } catch (e) { t = null; }
  if (!t || t.role !== 'partner') return next();
  const deny = () => res.status(200).json({ ok: false, restricted: true, error: 'Not available for this login' });
  try {
    const u = (await dashUsersLoad())[t.uid];
    if (!u || !u.active || u.role !== 'partner' || Number(u.pass_ver) !== Number(t.pv)) {
      return res.status(401).json({ ok: false, error: 'This login is no longer active. Sign in again.' });
    }
    req.krwUser = { id: u.id, name: u.name, role: 'partner' };
    const path = req.path;
    if (path === '/auth/check' || path === '/auth/login') return next();
    if (req.method !== 'GET' || !PARTNER_GET_ALLOW.some(rx => rx.test(path))) return deny();
    // Scope the query: nothing that could widen the feed past its SSDI exclusion.
    const clean = {};
    PARTNER_QUERY_KEEP.forEach(k => { if (req.query && typeof req.query[k] === 'string') clean[k] = req.query[k]; });
    req.query = clean;
    req.url = path + (Object.keys(clean).length ? '?' + new URLSearchParams(clean).toString() : '');
    if (!API_KEY) return deny();
    req.headers['x-api-key'] = API_KEY;   // the allow-listed read may pass the admin guard
    const send = res.json.bind(res);
    res.json = (body) => {
      let out;
      try { out = partnerSanitize(path, body); } catch (e) { out = { ok: false, restricted: true, error: 'Not available for this login' }; }
      return send(out);
    };
    return next();
  } catch (e) {
    return res.status(401).json({ ok: false, error: 'Could not verify this login. Sign in again.' });
  }
});
console.log('[Auth] patch 388 - partner logins: read only, allow-listed, SSDI and billable approvals excluded');
// ── end patch 388 gate ───────────────────────────────────────────────────────

'''

USERS_ANCHOR = """app.get('/auth/config', requireKey, (req, res) => {
  res.json({ ok: true, lead_key: LEAD_KEY || '' });
});
"""
USERS_BLOCK = r'''// patch 388: Kyler manages partner logins from his own portal.
app.get('/auth/users', requireKey, async (req, res) => {
  try {
    await _dashUsersReady;
    const r = await pool.query(`SELECT id, name, role, active, created_at, last_login_at FROM dash_users ORDER BY id`);
    res.json({ ok: true, users: r.rows, pages: PARTNER_PAGES });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/auth/users', requireKey, async (req, res) => {
  try {
    await _dashUsersReady;
    const name = String((req.body && req.body.name) || '').trim().slice(0, 40);
    const password = String((req.body && req.body.password) || '');
    if (!name) return res.status(400).json({ ok: false, error: 'Name is required' });
    if (password.length < 10) return res.status(400).json({ ok: false, error: 'Password must be at least 10 characters' });
    if (DASH_PASSWORD && password === DASH_PASSWORD) return res.status(400).json({ ok: false, error: 'Pick a password that is different from yours' });
    const clash = await dashUserByPassword(password);
    const existing = (await pool.query(`SELECT id FROM dash_users WHERE lower(name) = lower($1) LIMIT 1`, [name])).rows[0];
    if (clash && (!existing || clash.id !== existing.id)) return res.status(400).json({ ok: false, error: 'Another login already uses that password. Pick a different one.' });
    const salt = _crypto288.randomBytes(16).toString('hex');
    const hash = await dashHash(password, salt);
    let row;
    if (existing) {
      row = (await pool.query(`UPDATE dash_users SET name=$1, pass_salt=$2, pass_hash=$3, pass_ver=pass_ver+1, active=true WHERE id=$4 RETURNING id, name, role, active`, [name, salt, hash, existing.id])).rows[0];
    } else {
      row = (await pool.query(`INSERT INTO dash_users (name, role, pass_salt, pass_hash) VALUES ($1,'partner',$2,$3) RETURNING id, name, role, active`, [name, salt, hash])).rows[0];
    }
    await dashUsersLoad(true);
    console.log(`[Auth] partner login ${existing ? 'reset' : 'created'}: ${row.name}`);
    res.json({ ok: true, user: row, reset: !!existing });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/auth/users/:id/active', requireKey, async (req, res) => {
  try {
    await _dashUsersReady;
    const on = !!(req.body && req.body.active === true);
    const r = await pool.query(`UPDATE dash_users SET active=$1, pass_ver=pass_ver + CASE WHEN $1 THEN 0 ELSE 1 END WHERE id=$2 RETURNING id, name, active`, [on, parseInt(req.params.id, 10) || 0]);
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'Login not found' });
    await dashUsersLoad(true);
    res.json({ ok: true, user: r.rows[0] });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
'''

edits = [
# token can carry who it belongs to
("""function mintToken(role) {
  const payload = JSON.stringify({ role, exp: Date.now() + TOKEN_TTL_MS });""",
 """function mintToken(role, extra) {   // patch 388: extra = { uid, name, pv } for partner logins
  const payload = JSON.stringify(Object.assign({ role, exp: Date.now() + TOKEN_TTL_MS }, extra || {}));"""),

# login falls through to the partner logins
("""    if (!ok) { await new Promise(r => setTimeout(r, 400)); return res.status(401).json({ ok: false, error: 'Wrong password' }); }
    res.json({ ok: true, token: mintToken('admin'), expires_in_hours: TOKEN_TTL_MS / 3600000 });""",
 """    if (!ok) {
      // patch 388: not the admin password - is it one of the partner logins?
      let u = null;
      try { u = await dashUserByPassword(given); } catch (e) { u = null; }
      if (u && u.role === 'partner') {
        pool.query(`UPDATE dash_users SET last_login_at=NOW() WHERE id=$1`, [u.id]).catch(() => {});
        return res.json({ ok: true, role: 'partner', name: u.name, token: mintToken('partner', { uid: u.id, name: u.name, pv: u.pass_ver }), expires_in_hours: TOKEN_TTL_MS / 3600000 });
      }
      await new Promise(r => setTimeout(r, 400)); return res.status(401).json({ ok: false, error: 'Wrong password' });
    }
    res.json({ ok: true, role: 'admin', token: mintToken('admin'), expires_in_hours: TOKEN_TTL_MS / 3600000 });"""),

# session check reports a partner
("""  const role = callerRole(req);
  res.json({ ok: !!role, role: role || null, login_available: !!DASH_PASSWORD });""",
 """  const role = callerRole(req);
  if (!role && req.krwUser) return res.json({ ok: true, role: 'partner', name: req.krwUser.name, pages: PARTNER_PAGES, login_available: !!DASH_PASSWORD });   // patch 388
  res.json({ ok: !!role, role: role || null, login_available: !!DASH_PASSWORD });"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for anc in (LOGIN_ANCHOR, USERS_ANCHOR):
    if src.count(anc)!=1: sys.exit("ABORT: block anchor found %d times: %s" % (src.count(anc), anc[:40]))
for a,r in edits: src=src.replace(a,r,1)
src = src.replace(USERS_ANCHOR, USERS_ANCHOR + USERS_BLOCK, 1)
src = src.replace(LOGIN_ANCHOR, PARTNER_BLOCK + LOGIN_ANCHOR, 1)
shutil.copyfile(FILE, FILE+".pre-388.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 388 applied")
