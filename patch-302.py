#!/usr/bin/env python3
"""
patch-302.py  --  The overnight janitor obeys the Leadbloom no-003 rule too

WHY
  Patch 299 took MVA-003-LT out of Leadbloom's LIVE ladder, and patch 301's
  manual resend route was built with the same rule. But the overnight
  janitor builds its own retry rungs, and its bottom rung was still
  "003 if under cap" for every publisher - including KRW-LEADBLOOM-MVA.
  One 6am run could have quietly sold a stuck Leadbloom lead to 003 at
  $1,700 the morning after Kyler banned exactly that.

WHAT IT CHANGES (janitorRun's rung builder only)
  - Leadbloom: never 003. An out-of-state stuck lead falls back to CH/LT
    without the state filter, mirroring the live ladder's overflow rung.
  - Every other publisher: unchanged - 003 under its 5/day cap, max 2 per
    janitor run, exactly as before.

Usage (from ~/krw/krw-backend):
    python3 patch-302.py
    node --check server.js
    git add -A && git commit -m "patch 302: janitor obeys Leadbloom no-003" && git push

Backup: server.js.pre-302.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 302" in src:
    sys.exit("patch 302 already applied - nothing to do")

OLD = """      const rungs = [];
      if (JAN_INTAKE_STATES.includes(st)) {
        if (process.env.LT_INTAKE_PASS && ltToday < chToday) rungs.push('LT-Intake', 'CH-Intake');
        else { rungs.push('CH-Intake'); if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake'); }
      }
      if (JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX) rungs.push('NLD CPA');   // patch 283
      if (lt003Today < 5 && sent003 < 2) rungs.push('MVA-003-LT');"""

if src.count(OLD) != 1:
    sys.exit("the janitor rung builder does not look the way patch 302 expects - aborting, server.js untouched")

NEW = """      const rungs = [];
      const janPushIntake = () => {
        if (process.env.LT_INTAKE_PASS && ltToday < chToday) rungs.push('LT-Intake', 'CH-Intake');
        else { rungs.push('CH-Intake'); if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake'); }
      };
      if (JAN_INTAKE_STATES.includes(st)) janPushIntake();
      if (JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX) rungs.push('NLD CPA');   // patch 283
      // patch 302 (Kyler, Oct 7): Leadbloom NEVER goes to 003 - the janitor
      // included. Its out-of-state stuck leads fall back to CH/LT without the
      // state filter, mirroring the live ladder's overflow rung (patch 299).
      if (rowPub === 'KRW-LEADBLOOM-MVA') {
        if (!JAN_INTAKE_STATES.includes(st)) janPushIntake();
      } else if (lt003Today < 5 && sent003 < 2) {
        rungs.push('MVA-003-LT');
      }"""

src = src.replace(OLD, NEW, 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-302.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 302 applied. backup: server.js.pre-302.bak")
print("  janitor: Leadbloom never retried to 003; out-of-state -> CH/LT instead")
print("  janitor: all other publishers unchanged")
