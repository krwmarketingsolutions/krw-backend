#!/usr/bin/env python3
"""
patch-351.py  --  DT-Intake narrowed to GA, TX, MO, FL; CH-Intake back as the
                  catch-all (Kyler, Oct 26 evening)

Routing on every path (live ladders, LA-HI line, janitor, resend-ladder):
    1. DT-Intake   GA, TX, MO, FL
    2. NLD CPA     its 11 states (AZ is NLD-first again), 10/day cap
    3. CH-Intake   anything neither of them took, any state - including NLD
                   rejects / over-cap and IL, WI, MI, IN, MN, TN, PA
LT-Intake stays off. 003 stays off. CH's catch-all applies to every
publisher, Leadbloom included (Kyler: "keep them going to CH-Intake if they
can't go to these two"). To take Leadbloom back off CH later, set
LB_CH_FALLBACK=false on Railway.

Requires patch 350. Backup: server.js.pre-351.bak. Safe to re-run: refuses twice.
"""
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd()), os.path.expanduser("~/krw")):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None


ROOT = find_root() or sys.exit("STOPPED: could not find krw-backend - run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()

if "patch 351" in src:
    sys.exit("patch 351 already applied - nothing to do")
if "patch 350" not in src:
    sys.exit("STOPPED: patch 350 is not applied - run patch-350.py first")

edits = []


def rep(label, old, new):
    edits.append((label, old, new))


# 1. live ladders
LADDER_START = "  // patch 350 (Kyler, Oct 26): DT-Intake replaces CH-Intake and LT-Intake as"
LADDER_END = "  const LADDER_FOR_PUB = PUB === 'KRW-LEADBLOOM-MVA' ? LB_ONLY_LADDER : LEADBLOOM_LADDER;"
NEW_LADDERS = r"""  // patch 351 (Kyler, Oct 26 evening): DT-Intake is contracted for GA, TX,
  // MO, FL only. NLD keeps its 11 states (AZ is NLD-first again). CH-Intake
  // comes back as the catch-all for anything neither of them takes - NLD
  // rejects and over-cap included - on every publisher, Leadbloom too
  // (LB_CH_FALLBACK=false on Railway takes Leadbloom back off CH). LT and
  // 003 stay off. Supersedes the patch 350 ladder.
  const DT_ON        = DT_INTAKE_ON();
  const DT_STATES    = DT_INTAKE_STATES();
  const CH_FOR_LB    = (process.env.LB_CH_FALLBACK || 'true') !== 'false';
  const NYC_LADDER = [
    { name: 'DT-Intake',  priority: 1, group: 'intake',   cap: null, payout: 2500, enabled: DT_ON,       states: DT_STATES },
    { name: 'NLD CPA',    priority: 2, group: 'nld',      cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318
    { name: 'CH-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2250, enabled: true,        states: 'ALL' },
    { name: 'MVA-003-LT', priority: 4, group: '003',      cap: 5,    payout: 1700, enabled: false,       states: 'ALL' },   // patch 305: 003 off
  ];
  const LEADBLOOM_LADDER = NYC_LADDER;   // the shared ladder: NYC, Kevin, everyone but Leadbloom
  const LB_ONLY_LADDER = [
    { name: 'DT-Intake',  priority: 1, group: 'intake',   cap: null, payout: 2500, enabled: DT_ON,       states: DT_STATES },
    { name: 'NLD CPA',    priority: 2, group: 'nld',      cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318
    { name: 'CH-Intake',  priority: 3, group: 'catchall', cap: null, payout: 2250, enabled: CH_FOR_LB,   states: 'ALL' },
  ];
  const LADDER_FOR_PUB = PUB === 'KRW-LEADBLOOM-MVA' ? LB_ONLY_LADDER : LEADBLOOM_LADDER;"""

# 2. shared DT state list next to DT_INTAKE_ON
rep("DT state list",
    "function DT_INTAKE_ON() { return (process.env.DT_INTAKE_ENABLED || 'true') !== 'false'; }",
    "function DT_INTAKE_ON() { return (process.env.DT_INTAKE_ENABLED || 'true') !== 'false'; }\n"
    "// patch 351: the states DT-Intake is contracted for. DT_INTAKE_STATES on Railway\n"
    "// (comma list, e.g. \"GA,TX,MO,FL,IL\") overrides without a code change.\n"
    "function DT_INTAKE_STATES() { const s = String(process.env.DT_INTAKE_STATES || 'GA,TX,MO,FL').toUpperCase().split(/[,\\s]+/).filter(Boolean); return s.length ? s : ['GA','TX','MO','FL']; }")

# 3. LA-HI line: DT for its states, CH for the rest of the intake list
rep("LA-HI states",
    "const MVA_INTAKE_STATES = ['FL','GA','WI','TX','MI','IN','IL','MN','CO','MO','NE','OK','TN','AZ'];   // patch 350: DT buys AZ",
    "const MVA_INTAKE_STATES = ['FL','GA','WI','TX','MI','IN','IL','MN','CO','MO','NE','OK','TN'];   // patch 351: back to the 13 intake states")
rep("LA-HI ladder",
    "  const ladder = [{ name: 'DT-Intake', enabled: DT_INTAKE_ON() }]   // patch 350: DT replaces CH/LT on this line",
    "  const ladder = [{ name: 'DT-Intake', enabled: DT_INTAKE_ON() && DT_INTAKE_STATES().includes(leadState) }, { name: 'CH-Intake', enabled: true }]   // patch 351: DT for its states, CH catches the rest")

# 4. janitor rungs
rep("janitor rungs",
    "      const dtFirst = rowPub === 'KRW-LEADBLOOM-MVA'\n"
    "        ? (st === 'AZ' || (JAN_INTAKE_STATES.includes(st) && st !== 'OK' && st !== 'NE'))\n"
    "        : (st === 'AZ' || JAN_INTAKE_STATES.includes(st));\n"
    "      if (dtOn && dtFirst) rungs.push('DT-Intake');\n"
    "      if (wantNld) rungs.push('NLD CPA');\n"
    "      if (dtOn && !rungs.includes('DT-Intake')) rungs.push('DT-Intake');",
    "      // patch 351: DT on its contracted states, NLD on its states, CH catches the rest\n"
    "      if (dtOn && DT_INTAKE_STATES().includes(st)) rungs.push('DT-Intake');\n"
    "      if (wantNld) rungs.push('NLD CPA');\n"
    "      if (rowPub !== 'KRW-LEADBLOOM-MVA' || (process.env.LB_CH_FALLBACK || 'true') !== 'false') rungs.push('CH-Intake');")

# 5. resend-ladder rungs
rep("resend rungs",
    "    const dtFirst = isLeadbloom\n"
    "      ? (st === 'AZ' || (JAN_INTAKE_STATES.includes(st) && st !== 'OK' && st !== 'NE'))\n"
    "      : (st === 'AZ' || JAN_INTAKE_STATES.includes(st));\n"
    "    if (dtOn && dtFirst) rungs.push('DT-Intake');\n"
    "    if (wantNld) rungs.push('NLD CPA');\n"
    "    if (dtOn) rungs.push('DT-Intake');",
    "    // patch 351: DT on its contracted states, NLD on its states, CH catches the rest\n"
    "    if (dtOn && DT_INTAKE_STATES().includes(st)) rungs.push('DT-Intake');\n"
    "    if (wantNld) rungs.push('NLD CPA');\n"
    "    if (!isLeadbloom || (process.env.LB_CH_FALLBACK || 'true') !== 'false') rungs.push('CH-Intake');")

# 6. funnel edges
rep("funnel edges",
    "        'KRW-KANTHONY-RS':  ['DT-Intake', 'NLD CPA'],\n        [LEADBLOOM_PUB_ID]: ['DT-Intake', 'NLD CPA'],\n        'KRW-NYC-MVA':      ['DT-Intake', 'NLD CPA'],",
    "        // patch 351: CH-Intake is the catch-all again\n        'KRW-KANTHONY-RS':  ['DT-Intake', 'NLD CPA', 'CH-Intake'],\n        [LEADBLOOM_PUB_ID]: ['DT-Intake', 'NLD CPA', 'CH-Intake'],\n        'KRW-NYC-MVA':      ['DT-Intake', 'NLD CPA', 'CH-Intake'],")

problems = []
for label, old, new in edits:
    n = src.count(old)
    if n != 1:
        problems.append("%s: anchor found %d time(s), expected 1" % (label, n))
for nm, s in (("ladder start", LADDER_START), ("ladder end", LADDER_END)):
    if src.count(s) != 1:
        problems.append("%s anchor found %d time(s), expected 1" % (nm, src.count(s)))
if not problems and src.index(LADDER_END) <= src.index(LADDER_START):
    problems.append("ladder markers out of order")
if problems:
    print("STOPPED - nothing changed. Anchors did not match:")
    for p in problems:
        print("  - " + p)
    sys.exit(1)

bak = FILE + ".pre-351.bak"
shutil.copyfile(FILE, bak)
a, b = src.index(LADDER_START), src.index(LADDER_END) + len(LADDER_END)
src = src[:a] + NEW_LADDERS + src[b:]
for label, old, new in edits:
    src = src.replace(old, new, 1)
open(FILE, "w", encoding="utf-8").write(src)
print("Patched OK: %s" % FILE)
print("Backup:     %s" % bak)
