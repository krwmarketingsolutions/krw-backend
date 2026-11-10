#!/usr/bin/env python3
r"""
patch-378.py  --  remove the daily cap on Eilers (MVA-003-LT)

Kyler, Nov 10: no caps on 003. Change the MVA-003-LT rung cap 5 -> null in
both the NYC and Leadbloom ladders so Eilers takes unlimited overflow (same
as LAG/CAG, cap: null). Still priority 4, so higher-payout buyers go first.

Backup: server.js.pre-378.bak    Safe to re-run: refuses twice.
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
if "patch 378" in src:
    sys.exit("patch 378 already applied - nothing to do")

edits = [
("    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376: Eilers reactivated nationwide @ $1800, bottom rung (higher-payout buyers go first)",
 "    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: null, payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376/378: Eilers nationwide @ $1800, bottom rung, uncapped (higher-payout buyers go first)"),
("    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376: Eilers catch-all for Leadbloom too",
 "    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: null, payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376/378: Eilers catch-all for Leadbloom too, uncapped"),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-378.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 378 applied")
