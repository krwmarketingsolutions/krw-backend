#!/usr/bin/env python3
"""
patch-303.py  --  Funnel page: the right copy of a re-sent lead wins

THE BUG (visible on the Funnel page today)
  The funnel de-dupes leads by phone per publisher, keeping the FIRST row the
  database happens to return - and the query has no ORDER BY, so "first" is
  arbitrary. A lead that was rejected once and then successfully re-sent
  exists as two rows with the same phone; whenever the dead copy wins the
  draw, the funnel shows the lead under "Rejected" and the buyer who actually
  bought it shows NO TRAFFIC. Today: Steven Bentley is sold to CH-Intake, but
  the funnel showed CH-Intake at zero and him under Rejected.

THE FIX
  Each funnel query now orders rows so the most meaningful copy of a phone
  number comes first and wins the de-dupe:
      billable > forwarded > buyer_rejected > anything else, newest first
  Pure display logic - the leads table, routing, portals and revenue numbers
  are untouched.

Usage (from ~/krw/krw-backend):
    python3 patch-303.py
    node --check server.js
    git add -A && git commit -m "patch 303: funnel dedupe picks the sold copy" && git push

Backup: server.js.pre-303.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 303" in src:
    sys.exit("patch 303 already applied - nothing to do")

LEAD_ORDER = """
       ORDER BY (billable IS TRUE) DESC, (status='forwarded') DESC, (status='buyer_rejected') DESC, id DESC"""

EDITS = [
    ("funnel MVA query",
     """         AND ${sinceClause}`,
      [Object.keys(mvaPubs)]""",
     """         AND ${sinceClause}""" + LEAD_ORDER + """`,
      [Object.keys(mvaPubs)]"""),
    ("funnel SSDI leads query",
     """         AND ${sinceClause}`,
      [Object.keys(ssdiPubs)]""",
     """         AND ${sinceClause}""" + LEAD_ORDER + """`,
      [Object.keys(ssdiPubs)]"""),
    ("funnel mass tort query",
     """         AND ${sinceClause}`,
      [Object.keys(massTortPubs)]""",
     """         AND ${sinceClause}""" + LEAD_ORDER + """`,
      [Object.keys(massTortPubs)]"""),
    ("funnel SSDI calls query",
     """       WHERE publisher_sub = 'KRW-JOSHUA-SIGNED'
         AND ${sinceClause}`
    );""",
     """       WHERE publisher_sub = 'KRW-JOSHUA-SIGNED'
         AND ${sinceClause}
       ORDER BY (billable IS TRUE) DESC, id DESC`
    );   // patch 303: billed copy of a redialed number wins the de-dupe"""),
]

for label, old, new in EDITS:
    n = src.count(old)
    if n != 1:
        sys.exit("%s: expected exactly 1 match, found %d - aborting, server.js untouched" % (label, n))
    src = src.replace(old, new, 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-303.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 303 applied. backup: server.js.pre-303.bak")
print("  all four funnel queries now rank billable > forwarded > buyer_rejected > rest, newest first")
