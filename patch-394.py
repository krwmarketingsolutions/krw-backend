#!/usr/bin/env python3
r"""
patch-394.py  --  partner Home: the buyer sheets table loads again

Kyler, Nov 12: Andrew's page was not loading. The funnel itself answers fine
for a partner login, but his Home buyer-sheets table came back empty: the
partner filter for /buyer-sheets/status looked up the sheet list
(BUYER_SHEETS) from a part of the file where that list is not in scope, so the
filter failed on every call and the server answered "Not available for this
login".

Edits (krw-backend/server.js):
  1. /buyer-sheets/status does the partner recount itself (MVA sheets only,
     MVA leads only, from the Oct 21 floor), where the sheet list is in scope.
  2. The partner filter for that route no longer touches the sheet list: it
     keeps MVA sheets, drops the retired ones and the service-account address,
     and zeroes the pending-approval figures.
  3. If the partner filter ever fails again it is logged, and an admin preview
     (?view_as=partner) is told why instead of a bare refusal.

Admin behaviour is unchanged.

Backup: server.js.pre-394.bak    Safe to re-run: refuses twice.
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
if "patch 394" in src:
    sys.exit("patch 394 already applied - nothing to do")

edits = [
# 2. the filter no longer needs the sheet list
("""      const out = [];
      for (const b of body.buyers) {
        if (String(b.vertical || '').toUpperCase() !== 'MVA') continue;
        if (['nld-ride', 'lt-intake', 'ch-intake', 'dt-intake'].includes(b.key)) continue;   // patch 390: not on a partner's buyer sheets
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
      body.buyers = out;""",
 """      // patch 394: the recount from the floor is done by the route itself (the
      // sheet list is only in scope there); a body that was not recounted is refused.
      if (body.partner_scoped !== true) return refuse;
      delete body.partner_scoped;
      body.buyers = body.buyers
        .filter(b => String(b.vertical || '').toUpperCase() === 'MVA' && !['nld-ride', 'lt-intake', 'ch-intake', 'dt-intake'].includes(b.key))   // patch 390: not on a partner's buyer sheets
        .map(b => Object.assign({}, b, { pending_approval: 0, pending_amount: 0 }));"""),

# 1. the route recounts for a partner
("""      if (cfg.hidden && req.query.all !== '1') continue;   // patch 392: retired buyers are not listed
""",
 """      if (cfg.hidden && req.query.all !== '1') continue;   // patch 392: retired buyers are not listed
      if (partnerReq && (cfg.hidden || String(cfg.vertical || '').toUpperCase() !== 'MVA')) continue;   // patch 394
"""),
("""    const out = [];
    for (const cfg of BUYER_SHEETS) {
      if (cfg.hidden && req.query.all !== '1') continue;""",
 """    const out = [];
    const partnerReq = !!(req.krwUser && req.krwUser.role === 'partner');   // patch 394: partner counts are MVA only, from the floor
    for (const cfg of BUYER_SHEETS) {
      if (cfg.hidden && req.query.all !== '1') continue;"""),
("""         FROM leads WHERE raw->>'buyer_name'=$1 AND status IN ('forwarded','buyer_rejected') AND received_at >= NOW() - INTERVAL '45 days'`, [cfg.buyer])).rows[0];
      const un = (await pool.query(`SELECT COUNT(*)::int AS n FROM buyer_sheet_rows""",
 """         FROM leads WHERE raw->>'buyer_name'=$1 AND status IN ('forwarded','buyer_rejected') AND ` +
        (partnerReq ? `campaign ILIKE 'mva%' AND received_at >= GREATEST(NOW() - INTERVAL '45 days', $2::timestamptz)` : `received_at >= NOW() - INTERVAL '45 days'`),
        partnerReq ? [cfg.buyer, PARTNER_SINCE_ISO] : [cfg.buyer])).rows[0];
      const un = (await pool.query(`SELECT COUNT(*)::int AS n FROM buyer_sheet_rows"""),
("""    res.json({ ok: true, scan_times_et: BS_SCAN_TIMES, service_account: (bsSaKey() || {}).client_email || null, buyers: out });""",
 """    if (partnerReq) return res.json({ ok: true, partner_scoped: true, scan_times_et: BS_SCAN_TIMES, buyers: out });   // patch 394
    res.json({ ok: true, scan_times_et: BS_SCAN_TIMES, service_account: (bsSaKey() || {}).client_email || null, buyers: out });"""),

# 3. a failing filter is logged, and an admin preview is told why
("""        .catch(() => ({ ok: false, restricted: true, error: 'Not available for this login' }))""",
 """        .catch((e) => {   // patch 394: never silent
          console.error('[Partner] filter failed on ' + path + ': ' + (e && e.message));
          const o = { ok: false, restricted: true, error: 'Not available for this login' };
          if (req.krwUser && req.krwUser.preview) o.debug = String((e && e.message) || e);
          return o;
        })"""),
]
for i,(x,r) in enumerate(edits,1):
    if src.count(x)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(x)))
for x,r in edits: src=src.replace(x,r,1)
shutil.copyfile(FILE, FILE+".pre-394.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 394 applied")
