#!/usr/bin/env python3
r"""
patch-376.py  --  reactivate Eilers Law Firm (MVA-003-LT) nationwide @ $1800

Kyler, Nov 10: Eilers is live again. They are buyer MVA-003-LT. Turn them
back on with their full statelist (nationwide / 'ALL') at a flat $1800, as
the bottom rung of the mva-nyc-split ladder so the higher-payout buyers
(NLD $2250, CAG $2500/2250/1800, LAG $2200) always get the lead first and
Eilers only takes what nobody above them bought.

Edits (krw-backend/server.js):
  1. NYC_LADDER MVA-003-LT rung: payout 1700 -> 1800, enabled false -> MVA003_LIVE()
  2. LB_ONLY_LADDER (Leadbloom): add the same MVA-003-LT catch-all rung
  3. define MVA003_LIVE() next to CAG_LIVE()/LAG_LIVE() (default ON; set
     MVA003_LIVE=false on Railway to pause without a redeploy)
  4. buyer-sheet scanner 'mva-003' billing amount 1700 -> 1800 (matches payout)

NOTE: the rung keeps its prior cap of 5 accepted/day. mva-funnel (the old
Tier-2 003 entry, states:[]) is left off - it has taken zero leads in 7+ days.

Backup: server.js.pre-376.bak    Safe to re-run: refuses twice.
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
if "patch 376" in src:
    sys.exit("patch 376 already applied - nothing to do")

edits = [
# 1. NYC_LADDER rung: on, $1800, env-gated
("    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1700, enabled: false,       states: 'ALL' },   // patch 305: 003 off",
 "    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376: Eilers reactivated nationwide @ $1800, bottom rung (higher-payout buyers go first)"),

# 2. LB_ONLY_LADDER: add the catch-all rung for Leadbloom's own traffic
("    { name: 'CAG',                  priority: 2, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 373: CAG 2nd behind NLD\n"
 "  ];   // patch 368: DT-Intake removed",
 "    { name: 'CAG',                  priority: 2, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 373: CAG 2nd behind NLD\n"
 "    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1800, enabled: MVA003_LIVE(), states: 'ALL' },   // patch 376: Eilers catch-all for Leadbloom too\n"
 "  ];   // patch 368: DT-Intake removed"),

# 3. MVA003_LIVE() toggle next to CAG_LIVE()
("function CAG_LIVE()  { return (process.env.CAG_LIVE || 'true') !== 'false'; }   // patch 371: LIVE (set CAG_LIVE=false on Railway to pause)",
 "function CAG_LIVE()  { return (process.env.CAG_LIVE || 'true') !== 'false'; }   // patch 371: LIVE (set CAG_LIVE=false on Railway to pause)\n"
 "function MVA003_LIVE() { return (process.env.MVA003_LIVE || 'true') !== 'false'; }   // patch 376: Eilers (MVA-003-LT) live (set MVA003_LIVE=false on Railway to pause)"),

# 4. billing scanner amount matches the $1800 payout
("  { key: 'mva-003',   label: 'MVA-003-LT', buyer: 'MVA-003-LT', vertical: 'MVA', sheet: '10sbja-_waUhHvWnu2t_K_WNHMiKOOE020-70yudDoLI', tab: null, amount: 1700, enabled: true },",
 "  { key: 'mva-003',   label: 'MVA-003-LT', buyer: 'MVA-003-LT', vertical: 'MVA', sheet: '10sbja-_waUhHvWnu2t_K_WNHMiKOOE020-70yudDoLI', tab: null, amount: 1800, enabled: true },   // patch 376: Eilers payout 1700 -> 1800"),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-376.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 376 applied")
