#!/usr/bin/env python3
r"""
patch-368.py  --  remove DT-Intake from the funnel; LAG becomes 2nd after NLD

Kyler (Nov 3): "pause and delete DT, we will not be using them anymore. Then
give [LAG] 2nd priority after NLD."

DT-Intake was already paused (patch 366: DT_INTAKE_ON()=false), so nothing has
routed to it since Oct 30. This removes DT from every active routing ladder so
it is no longer a buyer anywhere:
  * /leads/mva-nyc-split  NYC_LADDER and LB_ONLY_LADDER - both DT rungs gone
    (the priority-1 intake rung AND the priority-3 catch-all rung).
  * /leads/mva-intake (LA-HI) - DT dropped from its mini-ladder.

And it re-ranks the funnel so NLD is first and LAG is second:
  * NLD CPA -> priority 1 (its 11 states, cap 10/day) - first dibs everywhere
    it buys.
  * Legal Advocate Group -> priority 2, and its state list is opened up from
    GA/TX/SC to LAG's full contracted seven (AZ, NV, GA, TX, NM, SC, UT). In
    the four states LAG shares with NLD (AZ/NV/NM/UT) NLD takes the lead first
    and LAG catches the overflow/rejects; in GA/TX/SC (which NLD doesn't buy)
    LAG is the sole buyer, same as before. The ladder's existing de-dupe +
    cap logic already makes a lower rung catch what the rung above it leaves.

NOT touched (on purpose): DT's disposition read-back (BUYER_SHEETS 'dt-intake')
stays enabled so any lead already sent to DT before the pause that later signs
is still captured for billing. The dormant sendToDtIntake() helper and the
/leads/forward-to-dt-intake endpoint are left in place (unreferenced by any
ladder, gated off by DT_INTAKE_ON()=false) rather than ripped out.

  krw-backend/server.js (four edits).

Backup: server.js.pre-368.bak    Safe to re-run: refuses twice.
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
if "patch 368" in src:
    sys.exit("patch 368 already applied - nothing to do")

edits = []

# ── E1: NYC_LADDER - drop both DT rungs, NLD first, LAG second ───────────────
edits.append((
"  const NYC_LADDER = [\n"
"    { name: 'Legal Advocate Group', priority: 1, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 367: LAG takes GA/TX/SC (states NLD isn't buying), delivered to the LAG Google Sheet\n"
"    { name: 'DT-Intake',  priority: 1, group: 'intake',   cap: null, payout: 2500, enabled: DT_ON,       states: DT_STATES },\n"
"    { name: 'NLD CPA',    priority: 2, group: 'nld',      cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318\n"
"    { name: 'DT-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2500, enabled: DT_ON,       states: 'ALL' },   // patch 358: CH-Intake off, DT is the catch-all\n"
"    { name: 'MVA-003-LT', priority: 4, group: '003',      cap: 5,    payout: 1700, enabled: false,       states: 'ALL' },   // patch 305: 003 off\n"
"  ];",
"  const NYC_LADDER = [\n"
"    { name: 'NLD CPA',              priority: 1, group: 'nld', cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318 / 368: NLD first\n"
"    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD - sole buyer in GA/TX/SC, catches NLD's overflow in the shared states\n"
"    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1700, enabled: false,       states: 'ALL' },   // patch 305: 003 off\n"
"  ];   // patch 368: DT-Intake removed from the funnel (no longer a buyer)"))

# ── E2: LB_ONLY_LADDER - drop both DT rungs, NLD first, LAG second ───────────
edits.append((
"  const LB_ONLY_LADDER = [\n"
"    { name: 'Legal Advocate Group', priority: 1, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 367\n"
"    { name: 'DT-Intake',  priority: 1, group: 'intake',   cap: null, payout: 2500, enabled: DT_ON,       states: DT_STATES },\n"
"    { name: 'NLD CPA',    priority: 2, group: 'nld',      cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318\n"
"    { name: 'DT-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2500, enabled: DT_ON,       states: 'ALL' },   // patch 358: CH-Intake off, DT is the catch-all\n"
"  ];",
"  const LB_ONLY_LADDER = [\n"
"    { name: 'NLD CPA',              priority: 1, group: 'nld', cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318 / 368: NLD first\n"
"    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD\n"
"  ];   // patch 368: DT-Intake removed"))

# ── E3: /leads/mva-intake (LA-HI) mini-ladder - drop DT ──────────────────────
edits.append((
"  const ladder = [\n"
"      { name: 'Legal Advocate Group', enabled: LAG_LIVE() && LAG_STATES.includes(leadState) },   // patch 367: LAG takes its states on the LA-HI line too\n"
"      { name: 'DT-Intake', enabled: DT_INTAKE_ON() },   // patch 359: CH-Intake off, DT catches all\n"
"    ]",
"  const ladder = [\n"
"      { name: 'Legal Advocate Group', enabled: LAG_LIVE() && LAG_STATES.includes(leadState) },   // patch 367 / 368: LAG only (DT-Intake removed)\n"
"    ]"))

# ── E4: open LAG's state list to the full contracted seven ───────────────────
edits.append((
"const LAG_STATES    = ['GA', 'TX', 'SC'];",
"const LAG_STATES    = ['AZ', 'NV', 'GA', 'TX', 'NM', 'SC', 'UT'];   // patch 368: LAG's full contracted 7 (NLD takes the shared states first; LAG is 2nd)"))

for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit E%d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-368.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 368 applied to", FILE)
