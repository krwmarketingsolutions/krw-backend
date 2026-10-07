#!/usr/bin/env python3
"""
patch-308.py  --  The funnel reports REAL per-buyer flow, not a static map

THE PROBLEM (Kyler, Oct 7: "the funnel page is not updated correctly")
  /dashboard/funnel returns publisher totals and buyer totals, but no
  pub->buyer breakdown. The page's "Live routing (actual, from real data)"
  lines are drawn from the static `routing` capability map - every publisher
  connects to every buyer it COULD reach, whether or not a single lead
  flowed. Today 6 leads moved (Leadbloom -> 003 x2, Leadbloom -> CH x1,
  Lumrah -> 003 x3) and the page showed none of it as flow.

WHAT IT CHANGES
  1. The MVA aggregation now also builds `edges`: for each publisher, how
     many of the period's leads landed at each buyer (same de-dupe and same
     best-copy-wins ordering as patch 303, so a re-sent lead counts once, at
     the buyer who actually took it). Returned as mva.edges.
  2. SSDI and mass tort get edges too - trivial 1:1 (publisher -> its only
     buyer, count = forwarded).
  3. The static `routing` map is kept for the drag-to-plan feature, but
     MVA-003-LT is removed from it (003 is off for everyone, patch 305), so
     nothing suggests 003 is reachable.
  funnel.html is updated separately (patch 309) to draw live lines from
  `edges` instead of `routing`.

Usage (from ~/krw/krw-backend):
    python3 patch-308.py
    node --check server.js
    git add -A && git commit -m "patch 308: funnel edges" && git push

Backup: server.js.pre-308.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 308" in src:
    sys.exit("patch 308 already applied - nothing to do")

def must_replace(label, old, new):
    global src
    n = src.count(old)
    if n != 1:
        sys.exit("%s: expected exactly 1 match, found %d - aborting, server.js untouched" % (label, n))
    src = src.replace(old, new, 1)

# ── 1. MVA edges ──────────────────────────────────────────────────────────────
must_replace("mva init",
"""    const mva = { publishers: {}, buyers: {} };""",
"""    const mva = { publishers: {}, buyers: {}, edges: {} };   // patch 308: edges = real pub->buyer flow""")

must_replace("mva edge accumulation",
"""      const buyerName = (row.status !== 'rejected' && row.buyer_name) ? row.buyer_name : 'Rejected';
      if (!mva.buyers[buyerName]) mva.buyers[buyerName] = { received: 0, accepted: 0, revenue: 0 };
      mva.buyers[buyerName].received++;""",
"""      const buyerName = (row.status !== 'rejected' && row.buyer_name) ? row.buyer_name : 'Rejected';
      if (!mva.buyers[buyerName]) mva.buyers[buyerName] = { received: 0, accepted: 0, revenue: 0 };
      mva.buyers[buyerName].received++;
      // patch 308: count the real flow per publisher->buyer pair, so the
      // funnel can draw lines for traffic that actually happened.
      if (buyerName !== 'Rejected') {
        if (!mva.edges[row.publisher_sub]) mva.edges[row.publisher_sub] = {};
        mva.edges[row.publisher_sub][buyerName] = (mva.edges[row.publisher_sub][buyerName] || 0) + 1;
      }""")

# ── 2. SSDI + mass tort edges (1:1 lines) ────────────────────────────────────
must_replace("ssdi init",
"""    const ssdi = { publishers: {}, buyers: {} };""",
"""    const ssdi = { publishers: {}, buyers: {}, edges: {} };   // patch 308""")

must_replace("mass tort init",
"""    const mass_tort = { publishers: {}, buyers: {} };""",
"""    const mass_tort = { publishers: {}, buyers: {}, edges: {} };   // patch 308""")

must_replace("response block",
"""    res.json({
      ok: true,
      period,
      mva,
      ssdi,
      mass_tort,""",
"""    // patch 308: 1:1 sections - the edge is simply the publisher's forwarded
    // count into its only buyer.
    for (const [pid, p] of Object.entries(ssdi.publishers)) {
      if (p.forwarded > 0) ssdi.edges[pid] = { [p.buyer]: p.forwarded };
    }
    for (const [pid, p] of Object.entries(mass_tort.publishers)) {
      if (p.forwarded > 0) mass_tort.edges[pid] = { [p.buyer]: p.forwarded };
    }

    res.json({
      ok: true,
      period,
      mva,
      ssdi,
      mass_tort,""")

# ── 3. routing map: 003 is off (patch 305) ───────────────────────────────────
must_replace("routing map",
"""        'KRW-KANTHONY-RS':  ['CH-Intake', 'LT-Intake', 'NLD CPA', 'MVA-003-LT'], // rides the NYC ladder (patch 266)
        [LEADBLOOM_PUB_ID]: ['CH-Intake', 'LT-Intake', 'NLD CPA', 'MVA-003-LT'], // rides the NYC ladder (patch 267); one Leadbloom account (patch 268)
        'KRW-NYC-MVA':      ['CH-Intake', 'LT-Intake', 'NLD CPA', 'MVA-003-LT'], // ladder order (Sep 16)""",
"""        // patch 308: MVA-003-LT removed - 003 is off for every publisher (patch 305)
        'KRW-KANTHONY-RS':  ['CH-Intake', 'LT-Intake', 'NLD CPA'],
        [LEADBLOOM_PUB_ID]: ['CH-Intake', 'LT-Intake', 'NLD CPA'],
        'KRW-NYC-MVA':      ['CH-Intake', 'LT-Intake', 'NLD CPA'],""")

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-308.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 308 applied. backup: server.js.pre-308.bak")
print("  /dashboard/funnel now returns mva/ssdi/mass_tort.edges = real pub->buyer flow")
print("  routing map no longer lists 003")
