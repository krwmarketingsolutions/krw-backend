#!/usr/bin/env python3
r"""
patch-384.py  --  LJA-MVA-GOOGLE on the dashboard funnel

Kyler, Nov 11: add LJA to the publisher list on the funnel. LJA-MVA-GOOGLE
now rides the live MVA ladder (patch 382), so it gets its own publisher node
on /dashboard/funnel with the same buyer connections as the other MVA
publishers (NLD CPA, CAG, Legal Advocate Group, Eilers Law).

Edits (krw-backend/server.js):
  1. mvaPubs: add 'LJA-MVA-GOOGLE': 'LJA (Google)'.
  2. routing edges: LJA-MVA-GOOGLE -> NLD / CAG / LAG / Eilers.

Backup: server.js.pre-384.bak    Safe to re-run: refuses twice.
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
if "patch 384" in src:
    sys.exit("patch 384 already applied - nothing to do")

edits = [
("""      'KRW-NYC-MVA': 'Lumrah LLC',
    };""",
"""      'KRW-NYC-MVA': 'Lumrah LLC',
      'LJA-MVA-GOOGLE': 'LJA (Google)',   // patch 384
    };"""),
("""        'KRW-NYC-MVA':      ['NLD CPA', 'CAG', 'Legal Advocate Group', 'Eilers Law'],   // patch 380
""",
"""        'KRW-NYC-MVA':      ['NLD CPA', 'CAG', 'Legal Advocate Group', 'Eilers Law'],   // patch 380
        'LJA-MVA-GOOGLE':   ['NLD CPA', 'CAG', 'Legal Advocate Group', 'Eilers Law'],   // patch 384
"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-384.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 384 applied")
