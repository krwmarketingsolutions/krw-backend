#!/usr/bin/env python3
"""
patch-299.py  --  Leadbloom never goes to MVA-003-LT again (Kyler, Oct 7)

THE DECISION
  Leadbloom's MVA traffic rides CH-Intake / LT-Intake 50-50, then NLD, and
  MVA-003-LT is removed from its ladder entirely. If a Leadbloom lead is in a
  state the intake buyers don't list and NLD declines it, it goes BACK to
  CH/LT without the state filter rather than dropping to 003 at $1,700 or
  going undelivered - Kyler's call: "push it through to LT or CH they will
  accept it".

  Every other publisher on this endpoint (KRW-NYC-MVA, KRW-KANTHONY-RS) keeps
  the existing four-rung ladder with 003 at the bottom. Nothing about Noah's
  or Kevin's routing changes.

LEADBLOOM LADDER AFTER THIS
  rung 1  CH-Intake $2,250 / LT-Intake $2,500, 50-50, the 13 intake states
  rung 2  NLD CPA   $2,000, cap 10/day, its 11 states
  rung 3  CH-Intake / LT-Intake again, ANY state (the overflow rung)
  (no 003, ever)

  So an MT or ND lead is offered to NLD first at $2,000, and only if NLD says
  no does it go to intake out-of-state. An intake-state lead is unaffected -
  see the dedupe below.

THE DEDUPE (why it is needed)
  CH and LT now appear twice in the Leadbloom ladder: once on the state rung
  and once on the nationwide overflow rung. For a lead that IS in an intake
  state both entries are eligible, which would post the same lead to the same
  buyer twice - the second time to a buyer that just declined it. The ladder
  is now deduplicated by buyer name, keeping each buyer's best (lowest)
  priority appearance, so the overflow rung only ever fires for a buyer that
  was not already tried.

Usage (from ~/krw/krw-backend):
    python3 patch-299.py
    node --check server.js
    git add -A && git commit -m "patch 299: Leadbloom off 003" && git push

Backup: server.js.pre-299.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 299" in src:
    sys.exit("patch 299 already applied - nothing to do")

# ── 1. the ladder: add the Leadbloom variant ─────────────────────────────────
OLD_LADDER = """  const NYC_LADDER = [
    { name: 'CH-Intake',  priority: 1, group: 'intake', cap: null, payout: 2250, enabled: true,                        states: INTAKE_STATES },
    { name: 'LT-Intake',  priority: 1, group: 'intake', cap: null, payout: 2500, enabled: !!process.env.LT_INTAKE_PASS, states: INTAKE_STATES },
    { name: 'NLD CPA',    priority: 2, group: 'nld',    cap: 10,   payout: 2000, enabled: true,                 states: NLD_ONLY_STATES },
    { name: 'MVA-003-LT', priority: 3, group: '003',    cap: 5,    payout: 1700, enabled: true,                 states: 'ALL' },
  ];"""

if src.count(OLD_LADDER) != 1:
    sys.exit("the NYC_LADDER block does not look the way patch 299 expects - aborting, server.js untouched")

NEW_LADDER = OLD_LADDER + """

  // patch 299 (Kyler, Oct 7): Leadbloom is OFF MVA-003-LT completely. CH and
  // LT take its out-of-state leads as the bottom rung instead, so nothing on
  // this line sells at $1,700 and nothing goes undelivered for want of a
  // nationwide buyer. Order still puts NLD ($2,000) ahead of the out-of-state
  // intake attempt, so a lead is only pushed outside a buyer's stated states
  // once the buyer who does cover that state has passed on it.
  const LEADBLOOM_LADDER = [
    { name: 'CH-Intake',  priority: 1, group: 'intake', cap: null, payout: 2250, enabled: true,                        states: INTAKE_STATES },
    { name: 'LT-Intake',  priority: 1, group: 'intake', cap: null, payout: 2500, enabled: !!process.env.LT_INTAKE_PASS, states: INTAKE_STATES },
    { name: 'NLD CPA',    priority: 2, group: 'nld',    cap: 10,   payout: 2000, enabled: true,                 states: NLD_ONLY_STATES },
    { name: 'CH-Intake',  priority: 3, group: 'intake-overflow', cap: null, payout: 2250, enabled: true,                        states: 'ALL' },
    { name: 'LT-Intake',  priority: 3, group: 'intake-overflow', cap: null, payout: 2500, enabled: !!process.env.LT_INTAKE_PASS, states: 'ALL' },
  ];
  const LADDER_FOR_PUB = PUB === 'KRW-LEADBLOOM-MVA' ? LEADBLOOM_LADDER : NYC_LADDER;"""

src = src.replace(OLD_LADDER, NEW_LADDER, 1)

# ── 2. build the ladder from the per-publisher list ──────────────────────────
OLD_FILTER = "  const eligible = NYC_LADDER.filter(buyer => {"
if src.count(OLD_FILTER) != 1:
    sys.exit("could not find the eligible filter - aborting, server.js untouched")
src = src.replace(OLD_FILTER, "  const eligible = LADDER_FOR_PUB.filter(buyer => {   // patch 299", 1)

# ── 3. dedupe by buyer name, best priority wins ──────────────────────────────
OLD_PLAN = "  const ladderPlan = eligible.map(x => x.name);"
if src.count(OLD_PLAN) != 1:
    sys.exit("could not find ladderPlan - aborting, server.js untouched")
NEW_PLAN = """  // patch 299: a buyer can appear on both its state rung and the nationwide
  // overflow rung. Keep only its first (best-priority) appearance so the
  // ladder never re-posts the same lead to a buyer that already declined it.
  const seenBuyer = new Set();
  const ladder = eligible.filter(x => { if (seenBuyer.has(x.name)) return false; seenBuyer.add(x.name); return true; });
  const ladderPlan = ladder.map(x => x.name);"""
src = src.replace(OLD_PLAN, NEW_PLAN, 1)

# ── 4. walk the deduped ladder ───────────────────────────────────────────────
OLD_LOOP = "  for (const buyer of eligible) {"
if src.count(OLD_LOOP) != 1:
    sys.exit("could not find the ladder loop - aborting, server.js untouched")
src = src.replace(OLD_LOOP, "  for (const buyer of ladder) {   // patch 299: deduped", 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-299.bak"))
open(TARGET, "w", encoding="utf-8").write(src)

print("patch 299 applied. backup: server.js.pre-299.bak")
print("  Leadbloom ladder: CH/LT (13 states) -> NLD -> CH/LT (any state). No 003.")
print("  Other publishers on this endpoint: unchanged, 003 still the bottom rung.")
print("  Ladder deduped by buyer so nobody is posted the same lead twice.")
print("")
print("next: node --check server.js && git add -A && git commit -m 'patch 299' && git push")
