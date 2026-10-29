#!/usr/bin/env python3
r"""
patch-363.py  --  allow a held billable-queue item to be approved

POST /billable-queue/:id/approve refused anything whose status wasn't exactly
'pending', so a case that had been Held could never be approved - there is no
unhold path, and invoicing only picks up status='approved'. A held case that
later signs was therefore stuck: payable in theory (held counts as owed) but
never invoiceable. Approve is a deliberate admin action, so it should override
a prior hold. Revenue is SET (= item.amount), not added, so approving an
already-signed lead does not double it.

  krw-backend/server.js (one edit): accept 'held' as well as 'pending'.

Backup: server.js.pre-363.bak    Safe to re-run: refuses twice.
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
if "patch 363" in src:
    sys.exit("patch 363 already applied - nothing to do")

anchor = ("    if (item.status !== 'pending') {\n"
          "      return res.status(400).json({ ok: false, error: `Already ${item.status}` });\n"
          "    }")
repl = ("    if (item.status !== 'pending' && item.status !== 'held') {   // patch 363: a held case can still be approved (approve overrides a prior hold; revenue is set, not added)\n"
        "      return res.status(400).json({ ok: false, error: `Already ${item.status}` });\n"
        "    }")

if src.count(anchor) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % src.count(anchor))
src = src.replace(anchor, repl, 1)
shutil.copyfile(FILE, FILE + ".pre-363.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 363 applied to", FILE)
