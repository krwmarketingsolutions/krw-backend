#!/usr/bin/env python3
r"""
patch-377.py  --  resend-ladder drops leftovers to Eilers (MVA-003-LT)

The /leads/:id/resend-ladder plan only ever tried DT-Intake + NLD (a stale
pre-Eilers plan). DT is dead and NLD is state-limited, so a held FL/OH/NC
lead had nowhere to go on resend and just re-held. Now that Eilers is the
nationwide catch-all (patch 376), append MVA-003-LT as the final rung of the
resend plan so leftovers actually reach an endpoint. NLD still goes first on
its states (higher payout); Eilers only takes what NLD doesn't.

Edit (krw-backend/server.js, inside /leads/:id/resend-ladder):

Backup: server.js.pre-377.bak    Safe to re-run: refuses twice.
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
if "patch 377" in src:
    sys.exit("patch 377 already applied - nothing to do")

edits = [
("    if (dtOn && !rungs.includes('DT-Intake')) rungs.push('DT-Intake');   // patch 359: DT catches the rest (CH off)\n"
 "    let plan = rungs.filter((x, i) => rungs.indexOf(x) === i);   // dedupe, order kept",
 "    if (dtOn && !rungs.includes('DT-Intake')) rungs.push('DT-Intake');   // patch 359: DT catches the rest (CH off)\n"
 "    if (typeof MVA003_LIVE === 'function' && MVA003_LIVE()) rungs.push('MVA-003-LT');   // patch 377: Eilers is the nationwide catch-all on resend too (bottom of the plan)\n"
 "    let plan = rungs.filter((x, i) => rungs.indexOf(x) === i);   // dedupe, order kept"),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-377.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 377 applied")
