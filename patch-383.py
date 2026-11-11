#!/usr/bin/env python3
r"""
patch-383.py  --  test leads are held, never sent to a buyer

Kyler, Nov 11: now that LJA-MVA-GOOGLE rides the live ladder (patch 382), a
test post would land in a real buyer's system. On /leads/mva-nyc-split, a lead
whose name contains DONOTCONTACT, or whose phone is a reserved 555 test number
(555-xxx-xxxx area code, or xxx-555-01xx), is saved but NOT sent anywhere. It
is held with a note showing which buyers it WOULD have gone to, so a test post
still verifies routing.

Backup: server.js.pre-383.bak    Safe to re-run: refuses twice.
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
if "patch 383" in src:
    sys.exit("patch 383 already applied - nothing to do")

edits = [
("""  if (!eligible.length) {
    const why = isNldState ?""",
"""  // patch 383: test leads (DONOTCONTACT name / reserved 555 test phone) are
  // never sent to a buyer - held with the plan they WOULD have followed.
  {
    const nm = ((b.first_name || '') + ' ' + (b.last_name || '')).toUpperCase();
    const ph = String(b.phone || '').replace(/\\D/g, '').slice(-10);
    if (nm.includes('DONOTCONTACT') || /^555\\d{7}$/.test(ph) || /^\\d{3}55501\\d{2}$/.test(ph)) {
      const plan = ladderPlan.length ? ladderPlan.map(n => n === 'MVA-003-LT' ? 'Eilers Law' : n).join(' -> ') : (isNldState ? 'NLD unavailable (would hold for review)' : 'nobody eligible');
      const why = 'TEST lead - not sent. Would route: ' + plan;
      const ct = await pool.connect();
      try { await ct.query("UPDATE leads SET status='received', buyer_error=$1 WHERE id=$2", [why, leadId]); } finally { ct.release(); }
      console.log(`[MVA-NYC-SPLIT] TEST ${b.first_name} ${b.last_name} | ${leadState} | ${why}`);
      return res.json({ ok: true, result: 'test', message: why, krw_id: leadId });
    }
  }

  if (!eligible.length) {
    const why = isNldState ?"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-383.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 383 applied")
