#!/usr/bin/env python3
r"""
patch-372.py  --  CAG Advertising: add the seven NEW states from Dylan's Nov 10 list

Dylan (Nov 10): "unlimited budget available for MVA campaigns in the following
states starting this week", with a per-state CPR table. The 13 states already
in the node are unchanged and keep their prices. Seven states are added, all at
$1,800 CPR: VA, NC, IL, NM, WA, GA, SC.

Full CAG list after this patch (20):
  $2,500  NY, NJ, PA
  $2,250  TX
  $1,800  AZ, IA, MT, NE, NV, ND, OK, UT, WY, VA, NC, IL, NM, WA, GA, SC

CAG_PAYOUT() already prices NY/NJ/PA at 2500, TX at 2250 and every other
CAG state at 1800, so only CAG_STATES changes. Priority is untouched: NLD
first, LAG second, CAG third, so in the states CAG shares with NLD and LAG
(AZ, NV, NM, UT, GA, TX, SC, IA, MT, NE, ND, OK, WY) CAG still catches
overflow and rejects. CAG becomes the SOLE live buyer in VA, NC, IL and WA
(nobody bought those before), plus NY, NJ, PA as before.

Backup: server.js.pre-372.bak    Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd()), os.path.expanduser("~/krw")):
        if os.path.isdir(os.path.join(c, "krw-backend")): return c
ROOT = find_root() or sys.exit("STOPPED: could not find krw-backend - run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()
if "patch 372" in src: sys.exit("patch 372 already applied - nothing to do")

OLD = "const CAG_STATES   = ['NY','NJ','PA','AZ','IA','MT','NE','NV','ND','OK','UT','WY','TX'];"
NEW = ("const CAG_STATES   = ['NY','NJ','PA','AZ','IA','MT','NE','NV','ND','OK','UT','WY','TX',\n"
       "                      'VA','NC','IL','NM','WA','GA','SC'];   // patch 372: + Dylan's 7 NEW states (Nov 10), all $1,800 CPR")
if src.count(OLD) != 1: sys.exit("STOPPED - CAG_STATES anchor found %d time(s), expected 1. Nothing changed." % src.count(OLD))

shutil.copyfile(FILE, FILE + ".pre-372.bak")
open(FILE, "w", encoding="utf-8").write(src.replace(OLD, NEW, 1))
print("Patched OK: %s" % FILE)
