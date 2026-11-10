#!/usr/bin/env python3
r"""
patch-380.py  --  funnel + portal display: 003 -> Eilers Law, drop CH/DT, add CAG/LAG

Kyler, Nov 10: remove CH-Intake and DT-Intake from the funnel, add CAG (and
LAG), and show 003 as "Eilers Law". The internal buyer key stays 'MVA-003-LT'
(billing + sheet scan unchanged); it is relabelled to 'Eilers Law' wherever
Kyler sees it - the portal lead feed and the funnel - so today's leads and the
funnel numbers reflect the new names with zero billing risk.

Edits (krw-backend/server.js):
  1. /leads/feed: relabel buyer_name MVA-003-LT -> Eilers Law.
  2. /dashboard/funnel mva query: same relabel.
  3. funnel known-buyer nodes: NLD CPA, CAG, Legal Advocate Group, Eilers Law.
  4. funnel hide set: hide retired CH-Intake / DT-Intake / old MVA-003-LT.
  5. funnel structural edges: each MVA publisher -> NLD/CAG/LAG/Eilers.

Backup: server.js.pre-380.bak    Safe to re-run: refuses twice.
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
if "patch 380" in src:
    sys.exit("patch 380 already applied - nothing to do")

edits = [
# 1. feed relabel
("""              status,zapier_status,buyer_intake_id,buyer_error,buyer_status,notes,publisher_sub,billable,revenue,
              raw->>'buyer_name' as buyer_name,""",
"""              status,zapier_status,buyer_intake_id,buyer_error,buyer_status,notes,publisher_sub,billable,revenue,
              CASE WHEN raw->>'buyer_name' = 'MVA-003-LT' THEN 'Eilers Law' ELSE raw->>'buyer_name' END as buyer_name,   -- patch 380: 003 shows as Eilers Law"""),

# 2. funnel mva query relabel
("""      `SELECT publisher_sub, status, billable, revenue, phone,
              raw->>'buyer_name' as buyer_name
       FROM leads""",
"""      `SELECT publisher_sub, status, billable, revenue, phone,
              CASE WHEN raw->>'buyer_name' = 'MVA-003-LT' THEN 'Eilers Law' ELSE raw->>'buyer_name' END as buyer_name
       FROM leads"""),

# 3. known buyer nodes
("""    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'DT-Intake'];   // patch 358: LT-Intake removed""",
"""    const mvaKnownBuyers = ['NLD CPA', 'CAG', 'Legal Advocate Group', 'Eilers Law'];   // patch 380: live ladder buyers (CH/DT retired, 003 -> Eilers Law)"""),

# 4. hide retired nodes
("""    const MVA_FUNNEL_HIDE = new Set(['LT-Intake']);   // patch 360: LT-Intake no longer a buyer - hide its node/edges even for historical leads""",
"""    const MVA_FUNNEL_HIDE = new Set(['LT-Intake', 'CH-Intake', 'DT-Intake', 'MVA-003-LT']);   // patch 380: hide retired buyers (003 already remapped to Eilers Law above)"""),

# 5. structural edges
("""        'KRW-KANTHONY-RS':  ['DT-Intake', 'NLD CPA'],   // patch 358: CH-Intake off
        [LEADBLOOM_PUB_ID]: ['DT-Intake', 'NLD CPA'],   // patch 358
        'KRW-NYC-MVA':      ['DT-Intake', 'NLD CPA'],   // patch 358""",
"""        'KRW-KANTHONY-RS':  ['NLD CPA', 'CAG', 'Legal Advocate Group', 'Eilers Law'],   // patch 380
        [LEADBLOOM_PUB_ID]: ['NLD CPA', 'CAG', 'Legal Advocate Group', 'Eilers Law'],   // patch 380
        'KRW-NYC-MVA':      ['NLD CPA', 'CAG', 'Legal Advocate Group', 'Eilers Law'],   // patch 380"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-380.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 380 applied")
