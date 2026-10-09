#!/usr/bin/env python3
"""
patch-321.py  --  Josh's MVA transfers keyed on source_id = VDCL

Follow-up to patch 320. Kyler's rule: a row on the LT dialer-export tab is
Joshua Duran's MVA transfer when its source_id column reads "VDCL" (Josh sent
the call). patch 320 keyed on the KramMarketing security_phrase; this makes
VDCL the primary signal and keeps KramMarketing as a fallback, so the scanner
files every VDCL row onto Josh's KRW-JOSHUA-MVA line via the same
bsUpsertKramMva path and the same scan cadence as every other MVA publisher.

  krw-backend/server.js
    1. bsMapHeader also locates the source_id column.
    2. Each scanned row carries source_id.
    3. The unmatched-branch ingest fires on source_id=VDCL OR KramMarketing.

Backup: server.js.pre-321.bak    Safe to re-run: refuses twice.
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
if not os.path.exists(FILE):
    sys.exit("krw-backend/server.js not found at " + FILE)

src = open(FILE, encoding="utf-8").read()
if "patch 321" in src or "r.sourceid" in src:
    sys.exit("patch 321 already applied to server.js - nothing to do")
if "bsUpsertKramMva" not in src:
    sys.exit("patch 320 must be applied first (bsUpsertKramMva not found)")

def apply(s, anchor, replacement, what):
    n = s.count(anchor)
    if n != 1:
        sys.exit("ABORT (%s): anchor found %d times, expected exactly 1. No files changed." % (what, n))
    return s.replace(anchor, replacement, 1)

# 1. bsMapHeader: also find the source_id column
A1 = "email: find(/^(EMAIL|E-?MAIL|EMAIL ADDRESS)$/, /EMAIL/) };"
R1 = "email: find(/^(EMAIL|E-?MAIL|EMAIL ADDRESS)$/, /EMAIL/), sourceid: find(/^SOURCE[_ ]?ID$/, /SOURCE.?ID/) };  // patch 321"
src = apply(src, A1, R1, "bsMapHeader source_id")

# 2. carry source_id onto each scanned row
A2 = "          secphrase: map.secphrase > -1 ? bsNorm(r[map.secphrase]) : '', state: map.state > -1 ? bsNorm(r[map.state]) : '', email: map.email > -1 ? bsNorm(r[map.email]) : '',"
R2 = ("          secphrase: map.secphrase > -1 ? bsNorm(r[map.secphrase]) : '', state: map.state > -1 ? bsNorm(r[map.state]) : '', email: map.email > -1 ? bsNorm(r[map.email]) : '',\n"
      "          sourceid: map.sourceid > -1 ? bsNorm(r[map.sourceid]) : '',  // patch 321: VDCL = Josh's transfer")
src = apply(src, A2, R2, "row carries source_id")

# 3. unmatched branch: fire on VDCL (primary) or KramMarketing (fallback)
A3 = "          if (/kram/i.test(r.secphrase || '')) {"
R3 = "          if (/^vdcl$/i.test(r.sourceid || '') || /kram/i.test(r.secphrase || '')) {   // patch 321: VDCL = Josh's MVA transfer"
src = apply(src, A3, R3, "VDCL ingest gate")

shutil.copyfile(FILE, FILE + ".pre-321.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 321 applied to", FILE)
