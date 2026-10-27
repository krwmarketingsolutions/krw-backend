#!/usr/bin/env python3
r"""
patch-355.py  --  fix SSDI dispo-update 500 (parameter $5 type)

POST /ssdi/dispo-update (and the sheet poller path) crashed with
"could not determine data type of parameter $5" on the audit INSERT:
$5 (payout) is used only in `CASE WHEN $5 IS NOT NULL`, so Postgres cannot
infer its type. Cast it to ::numeric in both spots so the insert runs.

  krw-backend/server.js -- the audit-trail INSERT VALUES block in
  app.post('/ssdi/dispo-update').

Backup: server.js.pre-355.bak    Safe to re-run: refuses twice.
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
if "patch 355" in src or "$5::numeric, $6, 'SSDI'" in src:
    sys.exit("patch 355 already applied - nothing to do")

A = ("         ($1, $2, $3, $4, $5, $6, 'SSDI', 'SSDI', $7, 'google_sheet',\n"
     "          CASE WHEN $5 IS NOT NULL THEN 'cpa' ELSE")
R = ("         ($1, $2, $3, $4, $5::numeric, $6, 'SSDI', 'SSDI', $7, 'google_sheet',   -- patch 355\n"
     "          CASE WHEN $5::numeric IS NOT NULL THEN 'cpa' ELSE")

n = src.count(A)
if n != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % n)
src = src.replace(A, R, 1)
shutil.copyfile(FILE, FILE + ".pre-355.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 355 applied to", FILE)
