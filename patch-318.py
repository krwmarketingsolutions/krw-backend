#!/usr/bin/env python3
"""
patch-318.py  --  PAUSE NLD as a buyer; those leads go to LT-Intake (Kyler, Oct 8)

WHAT CHANGES
  NLD CPA stops receiving leads on every live path:
    - /leads/mva-nyc-split ladder (all three ladders: NYC/shared, Leadbloom)
    - mva-funnel waterfall Tier 1 (states gated to none while paused)
    - NLD Ping (forwardToNldPing short-circuits before any ping goes out)
    - janitor retries (incl. the 6am 1028 retries - those now recover to LT)
    - manual /leads/:id/resend-ladder
  Where NLD would have taken the lead, LT-Intake takes it instead:
    - Leadbloom: LT only (its ladder is LT+NLD, so pausing NLD leaves LT)
    - NYC/Kevin: unchanged CH/LT 50/50; NLD-state leads ride the existing
      CH/LT nationwide overflow rung

  UNPAUSE: set NLD_PAUSED=false on Railway (no deploy needed beyond the
  variable change restarting the service), or apply a later patch.

  NOT gated: the separate mva-cpl $100 CPL line (campaign 33958) - dormant,
  zero traffic in the last 7 days, and its ping attempt IS gated via
  forwardToNldPing.

Usage (from ~/krw/krw-backend):
    python3 patch-318.py
    node --check server.js
    git add -A && git commit -m "patch 318: pause NLD, leads to LT" && git push

Backup: server.js.pre-318.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")
src = open(TARGET, encoding="utf-8").read()
if "patch 318" in src:
    sys.exit("patch 318 already applied - nothing to do")

def must(label, old, new, count=1):
    global src
    n = src.count(old)
    if n != count:
        sys.exit("%s: expected %d match(es), found %d - aborting, server.js untouched" % (label, count, n))
    src = src.replace(old, new)

# -- 1. the switch ------------------------------------------------------------
must("flag",
"""const MVA_BUYERS = [""",
"""// patch 318 (Kyler, Oct 8): NLD is PAUSED as a buyer. No lead goes to NLD
// from any live path (nyc-split ladders, funnel waterfall Tier 1, NLD Ping,
// janitor retries, manual resends) while this is true - LT-Intake takes
// what NLD would have taken. Unpause with NLD_PAUSED=false on Railway or a
// later patch.
const NLD_PAUSED = (process.env.NLD_PAUSED || 'true') === 'true';

const MVA_BUYERS = [""")

# -- 2. funnel waterfall Tier 1 -----------------------------------------------
must("waterfall states",
"""    states: ['UT','MT','WY','AZ','CA','NV','OK','NE','ND','IA','NM'], // NLD only accepts these states (PA removed, CA added - Kyler, Sep 15; this is the array MVA_BUYERS.find() actually uses, previous fix to a different unused variable never touched this)""",
"""    states: NLD_PAUSED ? [] : ['UT','MT','WY','AZ','CA','NV','OK','NE','ND','IA','NM'], // patch 318: empty while NLD is paused. NLD only accepts these states (PA removed, CA added - Kyler, Sep 15; this is the array MVA_BUYERS.find() actually uses)""")

# -- 3. NLD Ping short-circuit --------------------------------------------------
must("ping gate",
"""async function forwardToNldPing(b, publisherSub) {
  const zip = b.zip_code || b.zip;""",
"""async function forwardToNldPing(b, publisherSub) {
  if (NLD_PAUSED) return { routed: false, reason: 'nld_paused' };   // patch 318
  const zip = b.zip_code || b.zip;""")

# -- 4. nyc-split ladders -------------------------------------------------------
must("shared ladders NLD rung",
"""    { name: 'NLD CPA',    priority: 2, group: 'nld',    cap: 10,   payout: 2000, enabled: true,                 states: NLD_ONLY_STATES },""",
"""    { name: 'NLD CPA',    priority: 2, group: 'nld',    cap: 10,   payout: 2000, enabled: !NLD_PAUSED,          states: NLD_ONLY_STATES },   // patch 318""",
count=2)
must("leadbloom ladder NLD rung",
"""    { name: 'NLD CPA',   priority: 1, group: 'lb5050', cap: 10,   payout: 2000, enabled: true,                        states: NLD_ONLY_STATES },""",
"""    { name: 'NLD CPA',   priority: 1, group: 'lb5050', cap: 10,   payout: 2000, enabled: !NLD_PAUSED,                 states: NLD_ONLY_STATES },   // patch 318""")

# -- 5. janitor -----------------------------------------------------------------
must("janitor leadbloom wantNld",
"""        const wantNld = JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX;""",
"""        const wantNld = !NLD_PAUSED && JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX;   // patch 318""")
must("janitor shared NLD rung",
"""      if (JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX) rungs.push('NLD CPA');   // patch 283""",
"""      if (!NLD_PAUSED && JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX) rungs.push('NLD CPA');   // patch 283 + 318""")

# -- 6. manual resend -------------------------------------------------------------
must("resend leadbloom wantNld",
"""      const wantNld = NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10;""",
"""      const wantNld = !NLD_PAUSED && NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10;   // patch 318""")
must("resend shared NLD rung",
"""    if (NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10) rungs.push('NLD CPA');""",
"""    if (!NLD_PAUSED && NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10) rungs.push('NLD CPA');   // patch 318""")

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-318.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 318 applied. backup: server.js.pre-318.bak")
