#!/usr/bin/env python3
"""
patch-317.py  --  Leadbloom routes to LT-Intake and NLD ONLY, 50/50 (Kyler, Oct 8)

WHAT CHANGES
  1. LIVE LADDER (/leads/mva-nyc-split): leads posted by KRW-LEADBLOOM-MVA now
     ride a two-buyer ladder - LT-Intake (any state) and NLD CPA (its 11
     states, cap 10/day) - both on the same rung, so the existing same-rung
     sort gives the 50/50. The split balances LEADBLOOM'S OWN sends for the
     day, not the whole campaign's (LT also takes NYC volume, which would
     otherwise starve LT's side of the split). In a state NLD doesn't cover,
     LT takes the lead alone; if one buyer rejects, the other gets it in the
     same request. CH-Intake never sees a Leadbloom lead. Everyone else keeps
     the shared CH/LT -> NLD -> CH/LT ladder, unchanged.
  2. JANITOR: stuck/retried Leadbloom leads (state caps, NLD 1028 retries,
     received-but-never-routed) now go LT + NLD only - no CH fallback.
  3. MANUAL RESEND (/leads/:id/resend-ladder): same rule for Leadbloom.

  003 stays off for everyone (patch 305). CA/CO company block unchanged.
  PA daily cap unchanged.

Usage (from ~/krw/krw-backend):
    python3 patch-317.py
    node --check server.js
    git add -A && git commit -m "patch 317: Leadbloom -> LT + NLD only, 50/50" && git push

Backup: server.js.pre-317.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")
src = open(TARGET, encoding="utf-8").read()
if "patch 317" in src:
    sys.exit("patch 317 already applied - nothing to do")

def must(label, old, new):
    global src
    n = src.count(old)
    if n != 1:
        sys.exit("%s: expected 1 match, found %d - aborting, server.js untouched" % (label, n))
    src = src.replace(old, new, 1)

# -- 1. ladder selection: Leadbloom gets its own two-buyer ladder -------------
must("ladder select",
"""  const LADDER_FOR_PUB = LEADBLOOM_LADDER;""",
"""  // patch 317 (Kyler, Oct 8): Leadbloom goes to LT-Intake and NLD ONLY,
  // 50/50. Both sit on the same rung so the same-rung sort below does the
  // split; in a state NLD doesn't cover, LT takes the lead alone, and a
  // rejection by one falls to the other in the same request. CH never sees
  // a Leadbloom lead. Everyone else keeps the shared ladder above.
  const LB_ONLY_LADDER = [
    { name: 'LT-Intake', priority: 1, group: 'lb5050', cap: null, payout: 2500, enabled: !!process.env.LT_INTAKE_PASS, states: 'ALL' },
    { name: 'NLD CPA',   priority: 1, group: 'lb5050', cap: 10,   payout: 2000, enabled: true,                        states: NLD_ONLY_STATES },
  ];
  const LADDER_FOR_PUB = PUB === 'KRW-LEADBLOOM-MVA' ? LB_ONLY_LADDER : LEADBLOOM_LADDER;""")

# -- 2. Leadbloom-only counts for its 50/50 tiebreak --------------------------
must("lb counts",
"""  const todayCount = {}, lastAt = {};
  for (const r of countRes.rows) { todayCount[r.buyer] = r.n; lastAt[r.buyer] = r.last_at ? new Date(r.last_at).getTime() : 0; }""",
"""  const todayCount = {}, lastAt = {};
  for (const r of countRes.rows) { todayCount[r.buyer] = r.n; lastAt[r.buyer] = r.last_at ? new Date(r.last_at).getTime() : 0; }

  // patch 317: Leadbloom's 50/50 balances Leadbloom's own sends for the day.
  // The campaign-wide counts above still drive the caps and everyone else's
  // tiebreak - LT takes NYC volume too, and counting that against LT would
  // starve its side of the Leadbloom split.
  const lbCount = {}, lbLast = {};
  if (PUB === 'KRW-LEADBLOOM-MVA') {
    const lbRes = await pool.query(
      `SELECT raw->>'buyer_name' AS buyer, COUNT(*)::int AS n, MAX(received_at) AS last_at
       FROM leads
       WHERE campaign='mva-nyc-split' AND status='forwarded' AND publisher_sub='KRW-LEADBLOOM-MVA'
         AND (received_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date
       GROUP BY raw->>'buyer_name'`);
    for (const r of lbRes.rows) { lbCount[r.buyer] = r.n; lbLast[r.buyer] = r.last_at ? new Date(r.last_at).getTime() : 0; }
  }""")

must("sort tiebreak",
"""  }).sort((a, b2) => {
    if (a.priority !== b2.priority) return a.priority - b2.priority;
    // same rung: the buyer with fewer accepted today goes first; on a tie,
    // whoever did NOT get the most recent one - this is the 50/50
    const ca = todayCount[a.name] || 0, cb = todayCount[b2.name] || 0;
    if (ca !== cb) return ca - cb;
    return (lastAt[a.name] || 0) - (lastAt[b2.name] || 0);
  });""",
"""  }).sort((a, b2) => {
    if (a.priority !== b2.priority) return a.priority - b2.priority;
    // same rung: the buyer with fewer accepted today goes first; on a tie,
    // whoever did NOT get the most recent one - this is the 50/50.
    // patch 317: Leadbloom balances its own sends, not the campaign's.
    const tc = PUB === 'KRW-LEADBLOOM-MVA' ? lbCount : todayCount;
    const la = PUB === 'KRW-LEADBLOOM-MVA' ? lbLast : lastAt;
    const ca = tc[a.name] || 0, cb = tc[b2.name] || 0;
    if (ca !== cb) return ca - cb;
    return (la[a.name] || 0) - (la[b2.name] || 0);
  });""")

# -- 3. janitor: Leadbloom retries go LT + NLD only ---------------------------
must("janitor rungs",
"""      const rungs = [];
      const janPushIntake = () => {
        if (process.env.LT_INTAKE_PASS && ltToday < chToday) rungs.push('LT-Intake', 'CH-Intake');
        else { rungs.push('CH-Intake'); if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake'); }
      };
      if (JAN_INTAKE_STATES.includes(st)) janPushIntake();
      if (JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX) rungs.push('NLD CPA');   // patch 283
      // patch 305 (Kyler, Oct 7): 003 is off for EVERY publisher (extends
      // patch 302, which was Leadbloom-only). Stuck out-of-state leads fall
      // back to CH/LT without the state filter.
      if (!JAN_INTAKE_STATES.includes(st)) janPushIntake();""",
"""      const rungs = [];
      const janPushIntake = () => {
        if (process.env.LT_INTAKE_PASS && ltToday < chToday) rungs.push('LT-Intake', 'CH-Intake');
        else { rungs.push('CH-Intake'); if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake'); }
      };
      if (rowPub === 'KRW-LEADBLOOM-MVA') {
        // patch 317 (Kyler, Oct 8): Leadbloom rides LT + NLD only - CH never
        // sees its leads, even on janitor retries. Rough 50/50 by whichever
        // has fewer accepted today.
        const wantNld = JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX;
        const wantLt  = !!process.env.LT_INTAKE_PASS;
        if (wantNld && wantLt) { if (nldToday <= ltToday) rungs.push('NLD CPA', 'LT-Intake'); else rungs.push('LT-Intake', 'NLD CPA'); }
        else if (wantNld) rungs.push('NLD CPA');
        else if (wantLt)  rungs.push('LT-Intake');
      } else {
      if (JAN_INTAKE_STATES.includes(st)) janPushIntake();
      if (JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX) rungs.push('NLD CPA');   // patch 283
      // patch 305 (Kyler, Oct 7): 003 is off for EVERY publisher (extends
      // patch 302, which was Leadbloom-only). Stuck out-of-state leads fall
      // back to CH/LT without the state filter.
      if (!JAN_INTAKE_STATES.includes(st)) janPushIntake();
      }""")

# -- 4. manual resend: same rule ----------------------------------------------
must("resend rungs",
"""    const rungs = [];
    const pushIntake = () => {
      if (process.env.LT_INTAKE_PASS && ltToday < chToday) rungs.push('LT-Intake', 'CH-Intake');
      else { rungs.push('CH-Intake'); if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake'); }
    };
    if (JAN_INTAKE_STATES.includes(st)) pushIntake();
    if (NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10) rungs.push('NLD CPA');
    // patch 305: 003 is off for every publisher - out-of-state leads fall
    // back to CH/LT without the state filter.
    if (!JAN_INTAKE_STATES.includes(st)) pushIntake();""",
"""    const rungs = [];
    const pushIntake = () => {
      if (process.env.LT_INTAKE_PASS && ltToday < chToday) rungs.push('LT-Intake', 'CH-Intake');
      else { rungs.push('CH-Intake'); if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake'); }
    };
    if (isLeadbloom) {
      // patch 317 (Kyler, Oct 8): Leadbloom resends go to LT and NLD only.
      const wantNld = NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10;
      const wantLt  = !!process.env.LT_INTAKE_PASS;
      if (wantNld && wantLt) { if (nldToday <= ltToday) rungs.push('NLD CPA', 'LT-Intake'); else rungs.push('LT-Intake', 'NLD CPA'); }
      else if (wantNld) rungs.push('NLD CPA');
      else if (wantLt)  rungs.push('LT-Intake');
    } else {
    if (JAN_INTAKE_STATES.includes(st)) pushIntake();
    if (NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10) rungs.push('NLD CPA');
    // patch 305: 003 is off for every publisher - out-of-state leads fall
    // back to CH/LT without the state filter.
    if (!JAN_INTAKE_STATES.includes(st)) pushIntake();
    }""")

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-317.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 317 applied. backup: server.js.pre-317.bak")
