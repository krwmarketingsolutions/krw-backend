#!/usr/bin/env python3
r"""
patch-390.py  --  partner buyer-sheets view: hide four buyers; 003 reads "Eilers Law"

Kyler, Nov 12: remove these buyers from Andrew's buyer sheets table:
    NLD Rideshare, LT-Intake, CH-AD Roblox/Rideshare, DT-Intake
A partner login now gets only NLD CPA, Eilers Law and Legal Advocate Group
there. Kyler's own view is unchanged.

Also the buyer-sheets label for MVA-003-LT now reads "Eilers Law" (display
only - the key, buyer name and sheet stay the same), matching the rename
already done on the feed and funnel.

Backup: server.js.pre-390.bak    Safe to re-run: refuses twice.
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
if "patch 390" in src:
    sys.exit("patch 390 already applied - nothing to do")

edits = [
("""        if (String(b.vertical || '').toUpperCase() !== 'MVA') continue;""",
 """        if (String(b.vertical || '').toUpperCase() !== 'MVA') continue;
        if (['nld-ride', 'lt-intake', 'ch-intake', 'dt-intake'].includes(b.key)) continue;   // patch 390: not on a partner's buyer sheets"""),
("""key: 'mva-003',   label: 'MVA-003-LT', buyer: 'MVA-003-LT',""",
 """key: 'mva-003',   label: 'Eilers Law', buyer: 'MVA-003-LT',"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-390.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 390 applied")
