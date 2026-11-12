#!/usr/bin/env python3
r"""
patch-389.py  --  partner logins only see MVA leads from Oct 21, 2026 on

Kyler, Nov 12: Andrew may only see leads that came in on or after October 21.
Everything before that is Kyler's alone. He also only sees MVA: no rideshare,
roblox or other mass tort (and still no SSDI). MVA = a campaign whose name
starts with "mva".

A partner login now has a data floor (PARTNER_SINCE, default
2026-10-21 00:00 Pacific; override with PARTNER_DATA_SINCE). It is applied on
the server on every route a partner can reach:

  /leads/feed          SQL filter: MVA campaigns, received_at >= floor (+ a second check on the way out)
  /leads/detail/:id    refused for a non-MVA lead or one received before the floor
  /leads/summary       answered with MVA-only, floor-limited counts (the all-time total starts at the floor)
  /dashboard/funnel    every period and custom range is cut off at the floor; SSDI and mass tort sections removed
  /buyer-sheets/status MVA buyers only; sent / dispositioned / waiting recounted for MVA leads from the floor;
                       the service-account address is dropped
  /auth/check          tells the page the floor so it can label it

Admin behaviour is unchanged.

Backup: server.js.pre-389.bak    Safe to re-run: refuses twice.
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
if "patch 389" in src:
    sys.exit("patch 389 already applied - nothing to do")

# the whole partnerSanitize function is replaced with an async, floor-aware one
a = src.index("function partnerSanitize(path, body) {")
b = src.index("app.use(async (req, res, next) => {\n  let t = null;\n  try { t = readToken(_bearer(req)); } catch (e) { t = null; }", a)
NEW_SANITIZE = r'''// patch 389: a partner login only sees leads received on or after this moment.
const PARTNER_SINCE_ISO = (() => {
  const d = new Date(process.env.PARTNER_DATA_SINCE || '2026-10-21T00:00:00-07:00');
  return isNaN(d.getTime()) ? '2026-10-21T07:00:00.000Z' : d.toISOString();
})();
const PARTNER_SINCE_MS = Date.parse(PARTNER_SINCE_ISO);
async function partnerSanitize(path, body) {
  if (!body || typeof body !== 'object') return body;
  const refuse = { ok: false, restricted: true, error: 'Not available for this login' };
  const isSsdi = (x) => /ssdi/i.test(String((x && (x.vertical || '')) || '')) || /ssdi/i.test(String((x && (x.campaign || '')) || ''));
  const tooOld = (x) => { const t = Date.parse((x && x.received_at) || ''); return !(t >= PARTNER_SINCE_MS); };   // unknown date counts as too old
  const notMva = (x) => !/^mva/i.test(String((x && x.campaign) || ''));   // partner sees MVA only
  if (path === '/leads/feed') {
    if (Array.isArray(body.leads)) { body.leads = body.leads.filter(l => !isSsdi(l) && !tooOld(l) && !notMva(l)); body.count = body.leads.length; }
    return body;
  }
  if (/^\/leads\/detail\//.test(path)) {
    if (body.lead && (isSsdi(body.lead) || tooOld(body.lead) || notMva(body.lead))) return refuse;
    return body;
  }
  if (path === '/buyer-sheets/status') {
    delete body.service_account;
    if (Array.isArray(body.buyers)) {
      const out = [];
      for (const b of body.buyers) {
        if (String(b.vertical || '').toUpperCase() !== 'MVA') continue;
        const cfg = BUYER_SHEETS.find(c => c.key === b.key);
        if (!cfg) continue;
        const rec = (await pool.query(
          `SELECT COUNT(*)::int AS sent,
                  COUNT(*) FILTER (WHERE raw->'buyer_disposition' IS NOT NULL OR buyer_status IN ('Rejected','Signed','Retained','Returned'))::int AS dispositioned,
                  COUNT(*) FILTER (WHERE raw->'buyer_disposition' IS NULL AND buyer_status NOT IN ('Rejected','Signed','Retained','Returned','Test') AND received_at < NOW() - INTERVAL '3 days')::int AS waiting_3d
           FROM leads WHERE raw->>'buyer_name'=$1 AND status IN ('forwarded','buyer_rejected') AND campaign ILIKE 'mva%'
             AND received_at >= GREATEST(NOW() - INTERVAL '45 days', $2::timestamptz)`, [cfg.buyer, PARTNER_SINCE_ISO])).rows[0];
        out.push(Object.assign({}, b, { pending_approval: 0, pending_amount: 0, sent_45d: rec.sent, dispositioned_45d: rec.dispositioned, waiting_over_3d: rec.waiting_3d }));
      }
      body.buyers = out;
    }
    return body;
  }
  if (path === '/dashboard/funnel') {
    const mvaPubs = Object.keys((body.mva && body.mva.publishers) || {});
    delete body.ssdi; delete body.mass_tort;
    if (body.routing) Object.keys(body.routing).forEach(k => { if (!mvaPubs.includes(k)) delete body.routing[k]; });
    return body;
  }
  return body;
}
// /leads/summary for a partner: the same shape, counted from the floor only.
async function partnerLeadsSummary() {
  const base = `COALESCE(vertical,'') != 'SSDI' AND COALESCE(raw->>'excluded','') <> 'true' AND campaign ILIKE 'mva%' AND received_at >= $1::timestamptz`;
  const weekAgo = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
  const monthStart = new Date().toISOString().slice(0, 7) + '-01';
  const [todayQ, weekQ, monthQ, statusQ] = await Promise.all([
    pool.query(`SELECT campaign, COUNT(*) as count FROM leads WHERE (received_at AT TIME ZONE 'America/New_York')::date=(NOW() AT TIME ZONE 'America/New_York')::date AND ${base} GROUP BY campaign`, [PARTNER_SINCE_ISO]),
    pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$2 AND ${base}`, [PARTNER_SINCE_ISO, weekAgo]),
    pool.query(`SELECT COUNT(*) as count FROM leads WHERE received_at::date>=$2 AND ${base}`, [PARTNER_SINCE_ISO, monthStart]),
    pool.query(`SELECT status, COUNT(*) as count FROM leads WHERE ${base} GROUP BY status ORDER BY count DESC`, [PARTNER_SINCE_ISO]),
  ]);
  return { ok: true, today: todayQ.rows, week: parseInt((weekQ.rows[0] || {}).count || 0), month: parseInt((monthQ.rows[0] || {}).count || 0), by_status: statusQ.rows, since: PARTNER_SINCE_ISO };
}
'''
src = src[:a] + NEW_SANITIZE + src[b:]

edits = [
# the response wrapper waits for the (now async) filter; summary is answered here
("""    const send = res.json.bind(res);
    res.json = (body) => {
      let out;
      try { out = partnerSanitize(path, body); } catch (e) { out = { ok: false, restricted: true, error: 'Not available for this login' }; }
      return send(out);
    };
    return next();""",
 """    if (path === '/leads/summary') return res.json(await partnerLeadsSummary());   // patch 389: floor-limited counts
    const send = res.json.bind(res);
    res.json = (body) => {   // patch 389: the filter is async (it recounts from the floor)
      Promise.resolve().then(() => partnerSanitize(path, body))
        .catch(() => ({ ok: false, restricted: true, error: 'Not available for this login' }))
        .then(out => send(out));
      return res;
    };
    return next();"""),

# the page is told the floor
("""  if (!role && req.krwUser) return res.json({ ok: true, role: 'partner', name: req.krwUser.name, pages: PARTNER_PAGES, login_available: !!DASH_PASSWORD });   // patch 388""",
 """  if (!role && req.krwUser) return res.json({ ok: true, role: 'partner', name: req.krwUser.name, pages: PARTNER_PAGES, data_since: PARTNER_SINCE_ISO, login_available: !!DASH_PASSWORD });   // patch 388 / 389"""),

# feed: SQL floor for a partner
("""    if (status)   { where.push(`status=$${i++}`);   params.push(status); }

    // Support portal_id lookup""",
 """    if (status)   { where.push(`status=$${i++}`);   params.push(status); }
    if (req.krwUser && req.krwUser.role === 'partner') { where.push(`received_at >= $${i++}::timestamptz AND campaign ILIKE 'mva%'`); params.push(PARTNER_SINCE_ISO); }   // patch 389: MVA only, from the floor

    // Support portal_id lookup"""),

# funnel: every range is cut off at the floor for a partner
("""  else                         sinceClause = "received_at >= (date_trunc('week', (NOW() AT TIME ZONE 'America/New_York')) AT TIME ZONE 'America/New_York')"; // default: this week
""",
 """  else                         sinceClause = "received_at >= (date_trunc('week', (NOW() AT TIME ZONE 'America/New_York')) AT TIME ZONE 'America/New_York')"; // default: this week
  if (req.krwUser && req.krwUser.role === 'partner') sinceClause = `(${sinceClause}) AND received_at >= '${PARTNER_SINCE_ISO}'`;   // patch 389: partner data floor (a validated server constant)
"""),
]
for i,(x,r) in enumerate(edits,1):
    if src.count(x)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(x)))
for x,r in edits: src=src.replace(x,r,1)
shutil.copyfile(FILE, FILE+".pre-389.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 389 applied")
