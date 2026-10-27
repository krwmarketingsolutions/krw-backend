#!/usr/bin/env python3
"""
patch-352.py  --  DT-Intake adds IL, IN, MN (Kyler, Oct 26)

DT-Intake's contracted states become GA, TX, MO, FL, IL, IN, MN. Everything
else from patch 351 is unchanged: NLD on its 11 states, CH-Intake catches the
rest. DT_INTAKE_STATES on Railway still overrides this default.

Requires patch 351. Backup: server.js.pre-352.bak. Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd()), os.path.expanduser("~/krw")):
        if os.path.isdir(os.path.join(c, "krw-backend")): return c
ROOT = find_root() or sys.exit("STOPPED: could not find krw-backend - run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()
if "patch 352" in src: sys.exit("patch 352 already applied - nothing to do")
if "patch 351" not in src: sys.exit("STOPPED: patch 351 is not applied - run patch-351.py first")
OLD = "function DT_INTAKE_STATES() { const s = String(process.env.DT_INTAKE_STATES || 'GA,TX,MO,FL').toUpperCase().split(/[,\\s]+/).filter(Boolean); return s.length ? s : ['GA','TX','MO','FL']; }"
NEW = "function DT_INTAKE_STATES() { const s = String(process.env.DT_INTAKE_STATES || 'GA,TX,MO,FL,IL,IN,MN').toUpperCase().split(/[,\\s]+/).filter(Boolean); return s.length ? s : ['GA','TX','MO','FL','IL','IN','MN']; }   // patch 352: + IL, IN, MN"
if src.count(OLD) != 1: sys.exit("STOPPED - DT_INTAKE_STATES anchor found %d time(s), expected 1. Nothing changed." % src.count(OLD))
shutil.copyfile(FILE, FILE + ".pre-352.bak")
open(FILE, "w", encoding="utf-8").write(src.replace(OLD, NEW, 1))
print("Patched OK: %s" % FILE)
