#!/usr/bin/env python3
r"""
patch-392.py  --  retired buyers leave the buyer sheets table (for everyone)

Kyler, Nov 12: NLD Rideshare, LT-Intake, CH-AD Roblox/Rideshare and DT-Intake
are no longer buyers, so take them off the buyer sheets table on his view too.

Those four BUYER_SHEETS entries get hidden: true, and /buyer-sheets/status
leaves hidden sheets out (add ?all=1 to list them). Nothing is deleted and the
scanner still reads those sheets on schedule, so a late signing or rejection on
a lead that is still open with one of them is not missed.

Backup: server.js.pre-392.bak    Safe to re-run: refuses twice.
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
if "patch 392" in src:
    sys.exit("patch 392 already applied - nothing to do")

lines = src.split("\n")
for key in ("nld-ride", "lt-intake", "ch-intake", "dt-intake"):
    hits = [i for i, l in enumerate(lines) if ("{ key: '%s'," % key) in l and "sheet:" in l]
    if len(hits) != 1: sys.exit("ABORT: BUYER_SHEETS entry %s found %d times." % (key, len(hits)))
    l = lines[hits[0]]
    if l.count("enabled: true }") != 1: sys.exit("ABORT: %s entry has no 'enabled: true }'." % key)
    lines[hits[0]] = l.replace("enabled: true }", "enabled: true, hidden: true }") + ("   // patch 392: retired - off the table, still scanned" if "patch 392" not in l else "")
src = "\n".join(lines)

a = """app.get('/buyer-sheets/status', requireKey, async (req, res) => {
  try {
    const out = [];
    for (const cfg of BUYER_SHEETS) {
"""
if src.count(a) != 1: sys.exit("ABORT: status anchor found %d times." % src.count(a))
src = src.replace(a, a + "      if (cfg.hidden && req.query.all !== '1') continue;   // patch 392: retired buyers are not listed\n", 1)
shutil.copyfile(FILE, FILE + ".pre-392.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 392 applied")
