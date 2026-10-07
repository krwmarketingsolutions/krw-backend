#!/usr/bin/env python3
"""
patch-307.py  --  PA daily cap 3 -> 8 (Kyler, Oct 7)

WHY
  The 3/day PA cap existed because 003 was PA's only buyer and Noah's 003
  leads lost $100 each. With 003 off (patch 305), PA rides the overflow rung
  to CH-Intake ($2,250) / LT-Intake ($2,500) like every other state, so PA
  volume makes money now. Kyler: lift it to 8.

  NYC_PA_DAILY_CAP on Railway still overrides this default if ever set.

Usage (from ~/krw/krw-backend):
    python3 patch-307.py
    node --check server.js
    git add -A && git commit -m "patch 307: PA cap to 8" && git push

Backup: server.js.pre-307.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 307" in src:
    sys.exit("patch 307 already applied - nothing to do")

OLD = "  const NYC_STATE_CAPS = { PA: parseInt(process.env.NYC_PA_DAILY_CAP || '3', 10) };"
if src.count(OLD) != 1:
    sys.exit("the PA cap line does not look the way patch 307 expects - aborting, server.js untouched")

NEW = "  const NYC_STATE_CAPS = { PA: parseInt(process.env.NYC_PA_DAILY_CAP || '8', 10) };   // patch 307: was 3 - PA sells to CH/LT now that 003 is off"
src = src.replace(OLD, NEW, 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-307.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 307 applied. backup: server.js.pre-307.bak")
print("  PA daily cap default: 3 -> 8 (NYC_PA_DAILY_CAP on Railway still overrides)")
