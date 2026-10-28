#!/usr/bin/env python3
r"""
patch-360.py  --  fix two follow-ons from patch 358/359

1. LT-Intake still showed as a funnel buyer node because a historical lead
   carries buyer_name='LT-Intake'; dropping it from the seed list wasn't
   enough. Add an explicit funnel hide-set so its node and edges never render.

2. patch 358 disabled the whole 'ch-intake' buyer-sheet entry, but that entry
   was ALSO the only scanner for Chad's roblox/rideshare rows (campaigns
   roblox-mt, roblox-chad, rideshare-chad), which feed CH-AD - a buyer that is
   still live (LA-HI roblox/rideshare -> CH-AD). Re-point the entry at CH-AD and
   re-enable it: the roblox/rideshare rows still match by campaign, while
   CH-Intake MVA (matched by buyer_name='CH-Intake') is dropped. So CH-Intake
   the MVA intake is off the sheet, Chad's roblox/rideshare keeps syncing.

  krw-backend/server.js (three edits).

Backup: server.js.pre-360.bak    Safe to re-run: refuses twice.
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
if "patch 360" in src:
    sys.exit("patch 360 already applied - nothing to do")

edits = []

# E1 - declare the funnel hide-set next to mvaKnownBuyers
edits.append((
"    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'DT-Intake'];   // patch 358: LT-Intake removed",
"    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'DT-Intake'];   // patch 358: LT-Intake removed\n"
"    const MVA_FUNNEL_HIDE = new Set(['LT-Intake']);   // patch 360: LT-Intake no longer a buyer - hide its node/edges even for historical leads"))

# E2 - skip hidden buyers when building nodes/edges (publisher totals above are untouched)
edits.append((
"      const buyerName = (row.status !== 'rejected' && row.buyer_name) ? row.buyer_name : 'Rejected';\n"
"      if (!mva.buyers[buyerName]) mva.buyers[buyerName] = { received: 0, accepted: 0, revenue: 0 };",
"      const buyerName = (row.status !== 'rejected' && row.buyer_name) ? row.buyer_name : 'Rejected';\n"
"      if (MVA_FUNNEL_HIDE.has(buyerName)) continue;   // patch 360: LT-Intake removed from the funnel\n"
"      if (!mva.buyers[buyerName]) mva.buyers[buyerName] = { received: 0, accepted: 0, revenue: 0 };"))

# E3 - repoint the CH sheet entry to CH-AD roblox/rideshare, re-enable
edits.append((
"  { key: 'ch-intake', label: 'CH-Intake',  buyer: 'CH-Intake',  vertical: 'MVA', sheet: '1vlM4f8lqOHemrRZ1IhYdS9amU826GNV5GJS8nfGRgE0', tab: null, campaigns: ['roblox-mt','roblox-chad','rideshare-chad'], amount: 2250, enabled: false },   // patch 358: CH-Intake off",
"  { key: 'ch-intake', label: 'CH-AD Roblox/Rideshare', buyer: 'CH-AD', vertical: 'Rideshare', sheet: '1vlM4f8lqOHemrRZ1IhYdS9amU826GNV5GJS8nfGRgE0', tab: null, campaigns: ['roblox-mt','roblox-chad','rideshare-chad'], amount: 2250, enabled: true },   // patch 360: CH-Intake MVA off the sheet; Chad's roblox/rideshare (CH-AD) still scanned by campaign"))

for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit E%d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-360.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 360 applied to", FILE)
