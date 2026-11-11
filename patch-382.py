#!/usr/bin/env python3
r"""
patch-382.py  --  LJA-MVA-GOOGLE onto the live MVA ladder

Lead 2597 (LJA-MVA-GOOGLE, TX) held with "No buyer configured for state: TX".
Cause: /leads/mva-funnel only hands Kevin Anthony + Leadbloom to the live
ladder (NLD / CAG / LAG / Eilers). Every other publisher falls through to the
old Tier-2 MVA_BUYERS list, which has no live buyers, so nothing ever routes.

Edits (krw-backend/server.js):
  1. /leads/mva-funnel: LJA-MVA-GOOGLE is handed to the live ladder too.
  2. /leads/mva-nyc-split: LJA-MVA-GOOGLE is a known ladder publisher, so the
     lead keeps its own attribution (not lumped into KRW-NYC-MVA). It rides
     the shared ladder: NLD on NLD states, else CAG/LAG/Eilers by price.

NOT YET APPLIED to server.js - waiting on Kyler's go-ahead (Nov 11).

Backup: server.js.pre-382.bak    Safe to re-run: refuses twice.
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
if "patch 382" in src:
    sys.exit("patch 382 already applied - nothing to do")

edits = [
# 1. mva-funnel hands LJA to the live ladder
("  if (['KRW-KANTHONY-RS', 'KRW-LEADBLOOM-MVA'].includes(b.publisher_sub || '')) {   // + Leadbloom (patch 267)",
 "  if (['KRW-KANTHONY-RS', 'KRW-LEADBLOOM-MVA', 'LJA-MVA-GOOGLE'].includes(b.publisher_sub || '')) {   // + Leadbloom (patch 267), + LJA Google (patch 382)"),

# 2. ladder keeps LJA attribution
("  const LADDER_PUBS = ['KRW-KANTHONY-RS', 'KRW-LEADBLOOM-MVA']; // one Leadbloom account (patch 268)",
 "  const LADDER_PUBS = ['KRW-KANTHONY-RS', 'KRW-LEADBLOOM-MVA', 'LJA-MVA-GOOGLE']; // one Leadbloom account (patch 268); LJA Google (patch 382)"),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-382.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 382 applied")
