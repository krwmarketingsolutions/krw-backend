#!/usr/bin/env python3
"""
patch-323.py  --  never surface buyer / law-firm text on Josh's MVA transfers

Kyler's rule: Josh should see as much disposition detail as possible EXCEPT
anything that names a law firm or exposes our buyer/routing. The VICIdial
comments column occasionally carries free text like "LAW FIRM SERVICES".
This scrubs the disposition note at ingest: if the note matches a law-firm
or known-buyer pattern, it drops to the bare status (e.g. "Open — in
outreach" / "Rejected"), keeping every safe claimant-side reason
("already represented", "attorney represented", "not qualified", etc).

  krw-backend/server.js — one guard right after kramClassify() in
  bsUpsertKramMva. Re-scan LT-Intake after deploy to refresh stored notes.

Backup: server.js.pre-323.bak    Safe to re-run: refuses twice.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None
ROOT = find_root()
if not ROOT:
    sys.exit("could not find the krw folder (needs a krw-backend/ subdir) - run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()
if "patch 323" in src or "EXPOSE_BUYER" in src:
    sys.exit("patch 323 already applied to server.js - nothing to do")

A = "  const cls = kramClassify(r);"
R = ("  const cls = kramClassify(r);\n"
     "  // patch 323: never surface a law-firm name or our buyer/routing to the publisher.\n"
     "  // A matching note drops to the bare status; safe claimant reasons are kept.\n"
     "  const EXPOSE_BUYER = /law\\s*firm|lawfirm|lead\\s*tree|\\blt[- ]?intake\\b|\\bnld\\b|mva[- ]?003|\\b003\\b|ch[- ]?intake|email\\s*agency|lar[- ]?mva/i;\n"
     "  if (cls && EXPOSE_BUYER.test(cls.note || '')) cls.note = cls.status;")
n = src.count(A)
if n != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % n)
src = src.replace(A, R, 1)

shutil.copyfile(FILE, FILE + ".pre-323.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 323 applied to", FILE)
