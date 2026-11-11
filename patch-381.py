#!/usr/bin/env python3
r"""
patch-381.py  --  Eilers (MVA-003-LT) payout 1800 -> 1700

Kyler, Nov 11: Eilers nets KRW $1700 (part of the $1800 goes to a broker).
Set the payout to 1700 everywhere it is used:
  1. NYC_LADDER rung
  2. LB_ONLY_LADDER rung
  3. resend / push-through price bid
  4. buyer-sheet scanner billing amount
Eilers stays the bottom rung of the price bid (now $1700, below CAG's $1800 floor).

Backup: server.js.pre-381.bak    Safe to re-run: refuses twice.
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
if "patch 381" in src:
    sys.exit("patch 381 already applied - nothing to do")

edits = [
("    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: null, payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376/378: Eilers nationwide @ $1800, bottom rung, uncapped (higher-payout buyers go first)",
 "    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: null, payout: 1700, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376/378/381: Eilers nationwide @ $1700 net, bottom rung, uncapped (higher-payout buyers go first)"),
("    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: null, payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376/378: Eilers catch-all for Leadbloom too, uncapped",
 "    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: null, payout: 1700, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376/378/381: Eilers catch-all for Leadbloom too, uncapped, $1700 net"),
("    if (typeof MVA003_LIVE === 'function' && MVA003_LIVE()) bid.push({ name: 'MVA-003-LT', payout: 1800 });",
 "    if (typeof MVA003_LIVE === 'function' && MVA003_LIVE()) bid.push({ name: 'MVA-003-LT', payout: 1700 });   // patch 381"),
("tab: null, amount: 1800, enabled: true },   // patch 376: Eilers payout 1700 -> 1800",
 "tab: null, amount: 1700, enabled: true },   // patch 381: Eilers payout $1700 net (broker split)"),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-381.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 381 applied")
