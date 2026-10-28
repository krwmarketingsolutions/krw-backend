#!/usr/bin/env python3
r"""
patch-358.py  --  turn CH-Intake fully off, DT-Intake becomes the catch-all;
                  remove LT-Intake from the funnel; stop scanning CH's sheet

Kyler (Oct 28): CH-Intake and LT-Intake are no longer with us.
  - Live MVA routing: CH-Intake off. DT-Intake takes its contracted states
    first (unchanged), NLD keeps its 11 states (unchanged), and DT-Intake is
    now the catch-all for everything else (states: 'ALL') in place of CH.
  - Funnel: LT-Intake removed from the buyer node list; CH-Intake dropped from
    the active pub->buyer routing edges (its node stays for history).
  - Buyer-sheet scanner: CH-Intake sheet disabled.

  krw-backend/server.js (five edits).

Backup: server.js.pre-358.bak    Safe to re-run: refuses twice.
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
if "patch 358" in src:
    sys.exit("patch 358 already applied - nothing to do")

edits = []

# E1 - NYC/shared ladder: CH-Intake catch-all -> DT-Intake catch-all
edits.append((
"    { name: 'CH-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2250, enabled: true,        states: 'ALL' },",
"    { name: 'DT-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2500, enabled: DT_ON,       states: 'ALL' },   // patch 358: CH-Intake off, DT is the catch-all"))

# E2 - Leadbloom ladder: CH-Intake catch-all -> DT-Intake catch-all
edits.append((
"    { name: 'CH-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2250, enabled: CH_FOR_LB,   states: 'ALL' },",
"    { name: 'DT-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2500, enabled: DT_ON,       states: 'ALL' },   // patch 358: CH-Intake off, DT is the catch-all"))

# E3 - funnel buyer nodes: drop LT-Intake
edits.append((
"    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'LT-Intake', 'DT-Intake'];   // patch 350",
"    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'DT-Intake'];   // patch 358: LT-Intake removed"))

# E4 - funnel structural routing edges: CH-Intake off
edits.append((
"        'KRW-KANTHONY-RS':  ['DT-Intake', 'NLD CPA', 'CH-Intake'],\n"
"        [LEADBLOOM_PUB_ID]: ['DT-Intake', 'NLD CPA', 'CH-Intake'],\n"
"        'KRW-NYC-MVA':      ['DT-Intake', 'NLD CPA', 'CH-Intake'],",
"        'KRW-KANTHONY-RS':  ['DT-Intake', 'NLD CPA'],   // patch 358: CH-Intake off\n"
"        [LEADBLOOM_PUB_ID]: ['DT-Intake', 'NLD CPA'],   // patch 358\n"
"        'KRW-NYC-MVA':      ['DT-Intake', 'NLD CPA'],   // patch 358"))

# E5 - buyer-sheet scanner: disable CH-Intake sheet
edits.append((
"  { key: 'ch-intake', label: 'CH-Intake',  buyer: 'CH-Intake',  vertical: 'MVA', sheet: '1vlM4f8lqOHemrRZ1IhYdS9amU826GNV5GJS8nfGRgE0', tab: null, campaigns: ['roblox-mt','roblox-chad','rideshare-chad'], amount: 2250, enabled: true },",
"  { key: 'ch-intake', label: 'CH-Intake',  buyer: 'CH-Intake',  vertical: 'MVA', sheet: '1vlM4f8lqOHemrRZ1IhYdS9amU826GNV5GJS8nfGRgE0', tab: null, campaigns: ['roblox-mt','roblox-chad','rideshare-chad'], amount: 2250, enabled: false },   // patch 358: CH-Intake off"))

for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit E%d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-358.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 358 applied to", FILE)
