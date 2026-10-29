#!/usr/bin/env python3
"""
patch-365.py  --  onboarding stages: add "pending-setup" as the stage right
                  before live (Kyler, Oct 29)

Stage order becomes: prospect, pre-contract, contract-sent, pending-setup,
live, scaling. "signed", "setup" and "testing" are folded into pending-setup
(any existing rows on those stages are moved).

Requires patch 353. Backup: server.js.pre-365.bak. Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd()), os.path.expanduser("~/krw")):
        if os.path.isdir(os.path.join(c, "krw-backend")): return c
ROOT = find_root() or sys.exit("STOPPED: could not find krw-backend - run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()
if "patch 365" in src: sys.exit("patch 365 already applied - nothing to do")
if "patch 353" not in src: sys.exit("STOPPED: patch 353 is not applied first")
OLD = "const OB_STAGES = ['prospect', 'pre-contract', 'contract-sent', 'signed', 'setup', 'testing', 'live', 'scaling'];"
NEW = """const OB_STAGES = ['prospect', 'pre-contract', 'contract-sent', 'pending-setup', 'live', 'scaling'];   // patch 365: pending-setup is the step before live
const OB_STAGE_MIGRATE = { signed: 'pending-setup', setup: 'pending-setup', testing: 'pending-setup' };   // patch 365: old stages fold into pending-setup"""
OLD2 = "    const all = req.query.all === '1';\n    const r = await pool.query('SELECT * FROM onboarding_clients' + (all ? '' : ' WHERE archived IS NOT TRUE') + ' ORDER BY sort, id');"
NEW2 = "    for (const [o, n] of Object.entries(OB_STAGE_MIGRATE)) await pool.query('UPDATE onboarding_clients SET stage=$2 WHERE stage=$1', [o, n]);   // patch 365\n    const all = req.query.all === '1';\n    const r = await pool.query('SELECT * FROM onboarding_clients' + (all ? '' : ' WHERE archived IS NOT TRUE') + ' ORDER BY sort, id');"
for lbl, a in (("stages", OLD), ("list route", OLD2)):
    if src.count(a) != 1: sys.exit("STOPPED - %s anchor found %d time(s), expected 1. Nothing changed." % (lbl, src.count(a)))
shutil.copyfile(FILE, FILE + ".pre-365.bak")
open(FILE, "w", encoding="utf-8").write(src.replace(OLD, NEW, 1).replace(OLD2, NEW2, 1))
print("Patched OK: %s" % FILE)
