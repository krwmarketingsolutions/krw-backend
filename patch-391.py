#!/usr/bin/env python3
r"""
patch-391.py  --  partner logins never see leads that went to retired intake buyers

Kyler, Nov 12: Andrew should not see the leads that went to LT-Intake,
DT-Intake or CH-Intake either (he already does not see those buyers on the
buyer sheets table). A partner login now skips any lead whose buyer is one of
    LT-Intake, LT-Intake-transfer, DT-Intake, CH-Intake, CH-AD
on every route it can reach: the feed (SQL + response filter), lead detail,
the summary counts and the funnel's MVA section. Kyler's view is unchanged.

Backup: server.js.pre-391.bak    Safe to re-run: refuses twice.
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
if "patch 391" in src:
    sys.exit("patch 391 already applied - nothing to do")

edits = [
# the list + its SQL form (fixed server constants, no user input)
("""const PARTNER_SINCE_MS = Date.parse(PARTNER_SINCE_ISO);
""",
 """const PARTNER_SINCE_MS = Date.parse(PARTNER_SINCE_ISO);
// patch 391: buyers whose leads a partner login never sees
const PARTNER_BUYER_HIDE = ['LT-Intake', 'LT-Intake-transfer', 'DT-Intake', 'CH-Intake', 'CH-AD'];
const PARTNER_BUYER_SQL = `COALESCE(raw->>'buyer_name','') NOT IN (${PARTNER_BUYER_HIDE.map(b => "'" + b.replace(/'/g, "''") + "'").join(',')})`;
"""),

# response filters
("""  const notMva = (x) => !/^mva/i.test(String((x && x.campaign) || ''));   // partner sees MVA only""",
 """  const notMva = (x) => !/^mva/i.test(String((x && x.campaign) || ''));   // partner sees MVA only
  const hiddenBuyer = (x) => PARTNER_BUYER_HIDE.includes(String((x && (x.buyer_name || (x.raw && x.raw.buyer_name))) || ''));   // patch 391"""),
("""body.leads = body.leads.filter(l => !isSsdi(l) && !tooOld(l) && !notMva(l));""",
 """body.leads = body.leads.filter(l => !isSsdi(l) && !tooOld(l) && !notMva(l) && !hiddenBuyer(l));"""),
("""if (body.lead && (isSsdi(body.lead) || tooOld(body.lead) || notMva(body.lead))) return refuse;""",
 """if (body.lead && (isSsdi(body.lead) || tooOld(body.lead) || notMva(body.lead) || hiddenBuyer(body.lead))) return refuse;"""),

# summary counts
("""AND campaign ILIKE 'mva%' AND received_at >= $1::timestamptz`;""",
 """AND campaign ILIKE 'mva%' AND ${PARTNER_BUYER_SQL} AND received_at >= $1::timestamptz`;"""),

# feed SQL
("""where.push(`received_at >= $${i++}::timestamptz AND campaign ILIKE 'mva%'`); params.push(PARTNER_SINCE_ISO); }   // patch 389: MVA only, from the floor""",
 """where.push(`received_at >= $${i++}::timestamptz AND campaign ILIKE 'mva%' AND ${PARTNER_BUYER_SQL}`); params.push(PARTNER_SINCE_ISO); }   // patch 389 / 391: MVA only, from the floor, no retired intake buyers"""),

# funnel MVA section
("""         AND publisher_sub = ANY($1::text[]) AND COALESCE(raw->>'excluded','') <> 'true'
         AND ${sinceClause}
       ORDER BY (billable IS TRUE) DESC, (status='forwarded') DESC, (status='buyer_rejected') DESC, id DESC`,
      [Object.keys(mvaPubs)]""",
 """         AND publisher_sub = ANY($1::text[]) AND COALESCE(raw->>'excluded','') <> 'true'
         AND ${sinceClause}${(req.krwUser && req.krwUser.role === 'partner') ? ' AND ' + PARTNER_BUYER_SQL : ''}
       ORDER BY (billable IS TRUE) DESC, (status='forwarded') DESC, (status='buyer_rejected') DESC, id DESC`,
      [Object.keys(mvaPubs)]"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-391.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 391 applied")
