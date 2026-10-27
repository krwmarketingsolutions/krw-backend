#!/usr/bin/env python3
r"""
patch-354.py  --  add an "Under Investigation" disposition

A signed lead a buyer is disputing (withholding payment until they confirm it
is valid) had nowhere clean to live: the manual disposition endpoint only
accepted signed / pending / open / rejected. This adds a fifth option,
'investigation', which sets buyer_status = 'Under Investigation', turns
billable OFF (so it drops off the publisher payout and owed totals) and leaves
the lead delivered, so it stays visible rather than looking rejected or signed.

  krw-backend/server.js
    - add 'investigation' to the ALLOWED dispositions
    - add its branch (buyer_status 'Under Investigation', billable=false)

Backup: server.js.pre-354.bak    Safe to re-run: refuses twice.
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
if "patch 354" in src or "'investigation'" in src:
    sys.exit("patch 354 already applied - nothing to do")

A1 = "    const ALLOWED = ['signed', 'pending', 'open', 'rejected'];"
R1 = "    const ALLOWED = ['signed', 'pending', 'open', 'rejected', 'investigation'];   // patch 354"

A2 = ("    } else if (d === 'open') {\n"
      "      buyerStatus = 'Open - in outreach'; notes = note || 'Open - in outreach'; billable = false;\n"
      "    } else { // rejected")
R2 = ("    } else if (d === 'open') {\n"
      "      buyerStatus = 'Open - in outreach'; notes = note || 'Open - in outreach'; billable = false;\n"
      "    } else if (d === 'investigation') {   // patch 354\n"
      "      buyerStatus = 'Under Investigation'; notes = note || 'Under investigation - buyer withholding payment pending validity'; billable = false;\n"
      "    } else { // rejected")

for name, a in (("ALLOWED list", A1), ("disposition branch", A2)):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: anchor '%s' found %d times, expected 1. No change." % (name, n))

src = src.replace(A1, R1, 1).replace(A2, R2, 1)
shutil.copyfile(FILE, FILE + ".pre-354.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 354 applied to", FILE)
