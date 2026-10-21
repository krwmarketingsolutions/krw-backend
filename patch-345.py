#!/usr/bin/env python3
"""
patch-345.py  --  HOTFIX: /funding-summary throws, endpoint returns 500
                  (plus: a case signed today showed as -1d)

THE BREAK
patch 343 added the per-lead detail block inside the payables branch, and that
block reads fsCostKnown. fsCostKnown is a `const` declared further down the
same loop body (added by patch 341, line ~7803, used at ~7760). Reading a const
before its declaration is a temporal dead zone error, so every request to
/funding-summary threw:

    Cannot access 'fsCostKnown' before initialization

and the dashboard showed "Could not load". My mistake in 343: I placed a new
read above an existing declaration without checking the order.

THE FIX
Hoist the declaration to immediately after `rate` and `amt` are computed, which
is before any use of it. Nothing else moves: the manualOnly counters and the
`if (fsCostKnown)` guard around margin/lane stay exactly where they are, so
behaviour is unchanged once it runs.

Backup: server.js.pre-345.bak    Safe to re-run: refuses twice.
"""
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None


ROOT = find_root() or sys.exit("run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()

if "patch 345" in src:
    sys.exit("patch 345 already applied - nothing to do")

DECL = """      /* A hand-entered row only belongs in P&L and margin once its publisher
         cost is known; at zero cost it would read as 100% profit. */
      const fsCostKnown = !r.invoice_only || (!!r.publisher_sub && rate > 0);
"""
AMT = "      const amt = fsNum(r.amount);"

if src.count(DECL) != 1:
    sys.exit("fsCostKnown declaration found %d times, expected 1 - aborting" % src.count(DECL))
if src.count(AMT) != 1:
    sys.exit("amt line found %d times, expected 1 - aborting" % src.count(AMT))

# Remove it from its current position, then put it back above every use.
src = src.replace(DECL, "", 1)
src = src.replace(AMT, AMT + """

      /* patch 345: declared here, above every use. A hand-entered row only
         belongs in P&L and margin once its publisher cost is known; at zero
         cost it would read as 100% profit. */
      const fsCostKnown = !r.invoice_only || (!!r.publisher_sub && rate > 0);""", 1)

# Prove the order is right before writing.
if src.index("const fsCostKnown") > src.index("cost_known: fsCostKnown"):
    sys.exit("declaration still sits after its first use - aborting, nothing changed")

# Age is measured against midnight today, so a case signed this afternoon came
# out negative and rendered as "-1d". Clamp both age readouts at zero.
for old, new in (
    ("days_since_signed: dAge === null ? null : Math.floor(dAge),",
     "days_since_signed: dAge === null ? null : Math.max(0, Math.floor(dAge)),"),
    ("days_since_signed: age === null ? null : Math.floor(age)",
     "days_since_signed: age === null ? null : Math.max(0, Math.floor(age))"),
):
    if src.count(old) != 1:
        sys.exit("age clamp anchor found %d times, expected 1 - aborting" % src.count(old))
    src = src.replace(old, new, 1)

shutil.copyfile(FILE, FILE + ".pre-345.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 345 applied - fsCostKnown hoisted above its uses")
print("backup at server.js.pre-345.bak")
