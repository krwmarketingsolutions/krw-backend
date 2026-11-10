#!/usr/bin/env python3
r"""
patch-372.py  --  CAG moves to priority 2 (behind NLD), LAG to 3

Kyler (Nov 10): make CAG #2 behind NLD for incoming leads. The ladder dispatch
sorts by priority number, so this swaps CAG (was 3) and LAG (was 2):
  NLD CPA 1  ->  CAG 2  ->  LAG 3  ->  MVA-003 4 (off)
In the states CAG and LAG share (AZ, NV, UT, TX) CAG now gets first crack after
NLD and LAG catches what CAG leaves. Exclusive states are unchanged. Applied to
both mva-nyc-split ladders.

  krw-backend/server.js (four edits).

Backup: server.js.pre-372.bak    Safe to re-run: refuses twice.
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
if "patch 372" in src:
    sys.exit("patch 372 already applied - nothing to do")

edits = [
("    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD - sole buyer in GA/TX/SC, catches NLD's overflow in the shared states\n",
 "    { name: 'Legal Advocate Group', priority: 3, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 372: LAG now 3rd (CAG moved to 2nd behind NLD)\n"),
("    { name: 'CAG',                  priority: 3, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 369: CAG 3rd - sole buyer of NY/NJ/PA, catches NLD/LAG overflow in shared states\n",
 "    { name: 'CAG',                  priority: 2, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 372: CAG 2nd behind NLD - sole buyer of NY/NJ/PA, first after NLD in shared states\n"),
("    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD\n",
 "    { name: 'Legal Advocate Group', priority: 3, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 372: LAG now 3rd\n"),
("    { name: 'CAG',                  priority: 3, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 369\n",
 "    { name: 'CAG',                  priority: 2, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 372: CAG 2nd behind NLD\n"),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-372.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 372 applied to", FILE)
