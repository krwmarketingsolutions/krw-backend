#!/usr/bin/env python3
r"""
patch-366.py  --  pause DT-Intake (not set up yet)

Kyler (Oct 30): pause the DT-Intake line from getting anything until they are
set up. DT_INTAKE_ON() is the single gate every DT path checks - the live buyer
ladder (DT at rung 1 for its states AND the rung 3 catch-all), the janitor
re-route, the /leads/:id resend, the /leads/mva-intake balancer, and the
/leads/forward-to-dt-intake endpoint - so forcing it false pauses all of them
at once. Nothing is sent to DT while paused.

NOTE: since patch 358 DT is also the catch-all, and CH-Intake is off, so while
DT is paused any lead that is not in NLD's states (and not an NLD-eligible one
under cap) has no buyer and will be left as buyer_rejected / "no eligible
buyer". NLD's own states still route to NLD normally.

RESUME: revert this one line back to the env-based form and redeploy.

  krw-backend/server.js (one line).

Backup: server.js.pre-366.bak    Safe to re-run: refuses twice.
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
if "patch 366" in src:
    sys.exit("patch 366 already applied - nothing to do")

anchor = "function DT_INTAKE_ON() { return (process.env.DT_INTAKE_ENABLED || 'true') !== 'false'; }"
repl   = "function DT_INTAKE_ON() { return false; }   // patch 366: DT-Intake PAUSED until set up. RESUME: return (process.env.DT_INTAKE_ENABLED || 'true') !== 'false';"

if src.count(anchor) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % src.count(anchor))
src = src.replace(anchor, repl, 1)
shutil.copyfile(FILE, FILE + ".pre-366.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 366 applied to", FILE)
