#!/usr/bin/env python3
r"""
patch-359.py  --  finish turning CH-Intake off on the remaining routing paths

patch 358 took CH-Intake out of the primary buyer ladder and the funnel, but
three live paths still used CH-Intake as the catch-all. This makes DT-Intake
the catch-all on all of them so no new lead can land on CH:
  - /leads/mva-intake balancer (was DT for its states, CH for the rest)
  - the janitor re-route loop
  - the /leads/:id resend loop
and fixes the DT-Intake buyer note that still called CH the catch-all.

  krw-backend/server.js (four edits).

Backup: server.js.pre-359.bak    Safe to re-run: refuses twice.
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
if "patch 359" in src:
    sys.exit("patch 359 already applied - nothing to do")

edits = []

# E1 - /leads/mva-intake balancer: DT-only catch-all
edits.append((
"  const ladder = [{ name: 'DT-Intake', enabled: DT_INTAKE_ON() && DT_INTAKE_STATES().includes(leadState) }, { name: 'CH-Intake', enabled: true }]   // patch 351: DT for its states, CH catches the rest",
"  const ladder = [{ name: 'DT-Intake', enabled: DT_INTAKE_ON() }]   // patch 359: CH-Intake off, DT catches all"))

# E2 - janitor re-route loop: DT catch-all instead of CH (no dedupe here, so guard)
edits.append((
"      if (rowPub !== 'KRW-LEADBLOOM-MVA' || (process.env.LB_CH_FALLBACK || 'true') !== 'false') rungs.push('CH-Intake');",
"      if (dtOn && !rungs.includes('DT-Intake')) rungs.push('DT-Intake');   // patch 359: DT catches the rest (CH off)"))

# E3 - /leads/:id resend loop: DT catch-all instead of CH
edits.append((
"    if (!isLeadbloom || (process.env.LB_CH_FALLBACK || 'true') !== 'false') rungs.push('CH-Intake');",
"    if (dtOn && !rungs.includes('DT-Intake')) rungs.push('DT-Intake');   // patch 359: DT catches the rest (CH off)"))

# E4 - DT-Intake buyer note: catch-all is DT now, not CH
edits.append((
"      'States: GA, TX, MO, FL, IL, IN, MN (patch 352, Oct 26). AZ went back to NLD. CH-Intake is the catch-all for everything else.',",
"      'States: GA, TX, MO, FL, IL, IN, MN (patch 352, Oct 26). AZ went back to NLD. DT-Intake is the catch-all for everything else (patch 358).',"))

for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit E%d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-359.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 359 applied to", FILE)
