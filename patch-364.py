#!/usr/bin/env python3
r"""
patch-364.py  --  let a publisher's payout_rate be cleared (no value)

POST /publishers/:pub_id/payout-rate only accepted a non-negative number, so a
rate could be changed but never unset. Kyler wants Leadbloom's rate blank while
per-lead payouts are still being discussed - "no value", not $0 (which would
read as decided). Accept an empty / null / "clear" payout_rate and store NULL.

  krw-backend/server.js (one edit to the payout-rate handler).

Backup: server.js.pre-364.bak    Safe to re-run: refuses twice.
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
if "patch 364" in src:
    sys.exit("patch 364 already applied - nothing to do")

anchor = (
"    const rate = Number((req.body || {}).payout_rate);\n"
"    if (!(rate >= 0)) return res.status(400).json({ ok: false, error: 'payout_rate must be a non-negative number' });\n"
"    const r = await pool.query(\n"
"      'UPDATE publishers SET payout_rate=$1 WHERE pub_id=$2 RETURNING pub_id, name, payout_rate',\n"
"      [rate, req.params.pub_id]);\n"
"    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'Publisher not found' });\n"
"    console.log(`[Publisher] ${req.params.pub_id} payout_rate -> $${rate}`);")

repl = (
"    // patch 364: allow clearing the rate (no value / still to be discussed), not just a number\n"
"    const rawRate = (req.body || {}).payout_rate;\n"
"    const clear = rawRate === null || rawRate === undefined || String(rawRate).trim() === '' || /^(clear|none|null|tbd)$/i.test(String(rawRate).trim());\n"
"    let rate = null;\n"
"    if (!clear) {\n"
"      rate = Number(rawRate);\n"
"      if (!(rate >= 0)) return res.status(400).json({ ok: false, error: 'payout_rate must be a non-negative number, or empty to clear it' });\n"
"    }\n"
"    const r = await pool.query(\n"
"      'UPDATE publishers SET payout_rate=$1 WHERE pub_id=$2 RETURNING pub_id, name, payout_rate',\n"
"      [clear ? null : rate, req.params.pub_id]);\n"
"    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'Publisher not found' });\n"
"    console.log(`[Publisher] ${req.params.pub_id} payout_rate -> ${clear ? '(cleared)' : '$' + rate}`);")

if src.count(anchor) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % src.count(anchor))
src = src.replace(anchor, repl, 1)
shutil.copyfile(FILE, FILE + ".pre-364.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 364 applied to", FILE)
