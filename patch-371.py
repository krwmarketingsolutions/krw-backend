#!/usr/bin/env python3
r"""
patch-371.py  --  turn CAG Advertising live in the MVA funnel

Kyler (Nov 10): turn CAG live. Posting verified end-to-end against CAG's
endpoint (accept = "Lead Accepted", reject/dupe caught). Flip CAG_LIVE to
default ON so the priority-3 CAG rung is selected. Kill switch preserved:
set CAG_LIVE=false on Railway to pause.

  krw-backend/server.js (one line).

Backup: server.js.pre-371.bak    Safe to re-run: refuses twice.
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
if "patch 371" in src:
    sys.exit("patch 371 already applied - nothing to do")
anchor = "function CAG_LIVE()  { return (process.env.CAG_LIVE || 'false') !== 'false'; }   // default OFF until verified"
repl   = "function CAG_LIVE()  { return (process.env.CAG_LIVE || 'true') !== 'false'; }   // patch 371: LIVE (set CAG_LIVE=false on Railway to pause)"
if src.count(anchor) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1." % src.count(anchor))
src = src.replace(anchor, repl, 1)
shutil.copyfile(FILE, FILE + ".pre-371.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 371 applied to", FILE)
