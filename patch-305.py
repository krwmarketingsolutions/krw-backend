#!/usr/bin/env python3
"""
patch-305.py  --  MVA-003-LT is OFF. Every publisher rides the full ladder.

THE DECISION (Kyler, Oct 7)
  "turn off 003 completely, direct all leads to the flow from everyone else."
  Every MVA publisher now gets the ladder Leadbloom got in patch 299:
      rung 1  CH-Intake $2,250 / LT-Intake $2,500, 50-50, the 13 intake states
      rung 2  NLD CPA   $2,000, its 11 states, 10/day
      rung 3  CH-Intake / LT-Intake again, ANY state (overflow)
  MVA-003-LT is never offered a lead again, anywhere.

  The numbers behind it: 003 took 72 of Noah's leads in the last 21 days at
  $1,700 - and Noah is paid $1,800 flat per signed case, so every one of
  those was a $100 LOSS. PA (which had no buyer but 003) now rides the
  overflow rung to CH/LT like any other out-of-state lead.

EVERY 003 PATH, CLOSED
  1. /leads/mva-nyc-split live ladder   - everyone now uses the no-003 ladder;
     the old NYC_LADDER's 003 entry is also disabled in place as a belt.
  2. overnight janitor                  - the 003 retry rung is gone for all
     publishers, not just Leadbloom (was patch 302); out-of-state stuck leads
     fall back to CH/LT.
  3. POST /leads/:id/resend-ladder      - same.
  4. /leads/mva-funnel waterfall        - 003's state list emptied so
     MVA_BUYERS.find() can never match it. (Effectively dormant anyway -
     every active publisher is rerouted to the nyc-split ladder.)

  Left alone: the 003 buyer-sheet scanner stays ON so dispositions for the
  leads 003 already holds keep syncing, billables keep flowing to the
  approval queue, and the funnel keeps showing its historical volume. The
  per-state PA cap (3/day) also stays - lift it separately if PA volume
  should now run free into CH/LT.

Usage (from ~/krw/krw-backend):
    python3 patch-305.py
    node --check server.js
    git add -A && git commit -m "patch 305: 003 off for everyone" && git push

Backup: server.js.pre-305.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 305" in src:
    sys.exit("patch 305 already applied - nothing to do")

def must_replace(label, old, new):
    global src
    n = src.count(old)
    if n != 1:
        sys.exit("%s: expected exactly 1 match, found %d - aborting, server.js untouched" % (label, n))
    src = src.replace(old, new, 1)

# ── 1a. live ladder: everyone on the no-003 ladder ───────────────────────────
must_replace("nyc-split ladder selection",
"""  const LADDER_FOR_PUB = PUB === 'KRW-LEADBLOOM-MVA' ? LEADBLOOM_LADDER : NYC_LADDER;""",
"""  // patch 305 (Kyler, Oct 7): 003 is OFF for EVERYONE. Every publisher rides
  // the same ladder: CH/LT (intake states) -> NLD -> CH/LT (any state).
  // NYC_LADDER above is no longer selected; its 003 entry is also disabled.
  const LADDER_FOR_PUB = LEADBLOOM_LADDER;""")

# ── 1b. belt and braces: the old ladder's 003 entry can never fire ───────────
must_replace("NYC_LADDER 003 entry",
"""    { name: 'MVA-003-LT', priority: 3, group: '003',    cap: 5,    payout: 1700, enabled: true,                 states: 'ALL' },""",
"""    { name: 'MVA-003-LT', priority: 3, group: '003',    cap: 5,    payout: 1700, enabled: false,                states: 'ALL' },   // patch 305: 003 off""")

# ── 2. janitor: no 003 for anyone ────────────────────────────────────────────
must_replace("janitor 003 rung",
"""      // patch 302 (Kyler, Oct 7): Leadbloom NEVER goes to 003 - the janitor
      // included. Its out-of-state stuck leads fall back to CH/LT without the
      // state filter, mirroring the live ladder's overflow rung (patch 299).
      if (rowPub === 'KRW-LEADBLOOM-MVA') {
        if (!JAN_INTAKE_STATES.includes(st)) janPushIntake();
      } else if (lt003Today < 5 && sent003 < 2) {
        rungs.push('MVA-003-LT');
      }""",
"""      // patch 305 (Kyler, Oct 7): 003 is off for EVERY publisher (extends
      // patch 302, which was Leadbloom-only). Stuck out-of-state leads fall
      // back to CH/LT without the state filter.
      if (!JAN_INTAKE_STATES.includes(st)) janPushIntake();""")

# ── 3. resend-ladder route: same ─────────────────────────────────────────────
must_replace("resend route 003 rung",
"""    if (isLeadbloom) {
      // patch 299 rule: Leadbloom never goes to 003. Out-of-state leads fall
      // back to CH/LT without the state filter instead.
      if (!JAN_INTAKE_STATES.includes(st)) pushIntake();
    } else if (lt003Today < 5) {
      rungs.push('MVA-003-LT');
    }""",
"""    // patch 305: 003 is off for every publisher - out-of-state leads fall
    // back to CH/LT without the state filter.
    if (!JAN_INTAKE_STATES.includes(st)) pushIntake();""")

# ── 4. mva-funnel waterfall: 003 can never match ─────────────────────────────
must_replace("MVA_BUYERS 003 states",
"""    states: ['AL','AK','AZ','AR','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA',
              'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK',
              'OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'], // all states except CA, CO""",
"""    states: [],   // patch 305 (Kyler, Oct 7): 003 turned off completely - this entry can never match a lead again""")

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-305.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 305 applied. backup: server.js.pre-305.bak")
print("  003 removed from: live ladder, janitor, resend route, mva-funnel waterfall")
print("  003 disposition scanner left ON for leads it already holds")
