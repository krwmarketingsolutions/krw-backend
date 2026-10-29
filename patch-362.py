#!/usr/bin/env python3
r"""
patch-362.py  --  add a first-class "Docs Collected" disposition/stage

The rideshare (CH-AD / LA-HI) pipeline has a real stage between "Open - in
outreach" and "Signed": docs collected, intake pending. The system had no way
to represent it, so those leads sat as "Open - in outreach" (portal: "In
outreach") with no signal that the file is docs-complete and queued for intake.

Adds disposition 'docs_collected' to POST /leads/:id/disposition
(buyer_status "Docs Collected - Intake Needed", notes from the caller, billable
false, status forwarded - a forward/in-progress state, NOT rejected), and maps
that buyer_status to a "Docs collected" response word on the publisher portal.

  krw-backend/server.js (three edits).

Backup: server.js.pre-362.bak    Safe to re-run: refuses twice.
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
if "patch 362" in src:
    sys.exit("patch 362 already applied - nothing to do")

edits = []

# E1 - allow the new disposition
edits.append((
"    const ALLOWED = ['signed', 'pending', 'open', 'rejected', 'investigation'];   // patch 354",
"    const ALLOWED = ['signed', 'pending', 'open', 'docs_collected', 'rejected', 'investigation'];   // patch 362: + docs_collected"))

# E2 - handler branch (insert after the 'open' branch)
edits.append((
"      buyerStatus = 'Open - in outreach'; notes = note || 'Open - in outreach'; billable = false;\n"
"    } else if (d === 'investigation') {   // patch 354",
"      buyerStatus = 'Open - in outreach'; notes = note || 'Open - in outreach'; billable = false;\n"
"    } else if (d === 'docs_collected') {   // patch 362: docs in hand, intake pending - a forward/in-progress stage, not a signing\n"
"      buyerStatus = 'Docs Collected - Intake Needed'; notes = note || 'Docs collected - intake needed'; billable = false; status = 'forwarded';\n"
"    } else if (d === 'investigation') {   // patch 354"))

# E3 - publisher portal response word for the new stage
edits.append((
"  else if (/^pending/i.test(bs)) response = 'In outreach';",
"  else if (/^pending/i.test(bs)) response = 'In outreach';\n"
"  else if (/docs?\\s*collected/i.test(bs)) response = 'Docs collected';   // patch 362"))

for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit E%d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-362.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 362 applied to", FILE)
