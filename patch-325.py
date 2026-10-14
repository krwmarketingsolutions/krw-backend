#!/usr/bin/env python3
"""
patch-325.py  --  manual billable add endpoint (for hand-added signed cases)

Josh's signed cases that don't come through a postback (e.g. a Retained row
off the call log) used to be added to billable_queue by hand with psql. This
adds an admin endpoint so they can be added the same way from the dashboard
or an API call, with a duplicate guard so the same cid is never queued twice.

  POST /billable-queue/manual   (admin key)
    body { cid, publisher_sub, amount, status?, note?, lead_id? }
    status defaults to 'pending'; 'approved' or 'held' also accepted.

Backup: server.js.pre-325.bak    Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE=os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c,"krw-backend")): return c
    return None
ROOT=find_root() or sys.exit("run from ~/krw")
FILE=os.path.join(ROOT,"krw-backend","server.js")
src=open(FILE,encoding="utf-8").read()
if "billable-queue/manual" in src or "patch 325" in src:
    sys.exit("patch 325 already applied - nothing to do")

ANCHOR="// Admin-only: Approve - marks billable_queue approved AND updates the real"
ENDPOINT=r'''// patch 325: manually add a billable (hand-added signed cases, no postback)
app.post('/billable-queue/manual', requireKey, async (req, res) => {
  try {
    const { cid, publisher_sub, amount, status, note, lead_id } = req.body || {};
    const phone = String(cid || '').replace(/\D/g, '').slice(-10);
    if (phone.length !== 10) return res.status(400).json({ ok: false, error: 'cid (10-digit phone) required' });
    if (!publisher_sub) return res.status(400).json({ ok: false, error: 'publisher_sub required' });
    const amt = Number(amount);
    if (!(amt > 0)) return res.status(400).json({ ok: false, error: 'amount must be a positive number' });
    const st = ['pending', 'held', 'approved'].includes(status) ? status : 'pending';
    const dup = await pool.query(
      `SELECT id, status, amount FROM billable_queue WHERE RIGHT(regexp_replace(cid,'\D','','g'),10)=$1 AND publisher_sub=$2 LIMIT 1`,
      [phone, publisher_sub]);
    if (dup.rows.length) return res.status(409).json({ ok: false, error: 'already in queue for this publisher', existing: dup.rows[0] });
    const raw = JSON.stringify({ source: 'manual_add', note: note || null, added_at: new Date().toISOString() });
    const r = await pool.query(
      `INSERT INTO billable_queue (cid, amount, publisher_sub, lead_id, status, raw) VALUES ($1,$2,$3,$4,$5,$6::jsonb) RETURNING id, status`,
      [phone, amt, publisher_sub, lead_id || null, st, raw]);
    // If approved and tied to a real lead, mark that lead billable too
    if (st === 'approved' && lead_id) {
      await pool.query(`UPDATE leads SET billable=true, revenue=COALESCE(revenue,0)+$1 WHERE id=$2`, [amt, lead_id]).catch(()=>{});
    }
    console.log(`[Billable Manual] + q${r.rows[0].id} ${publisher_sub} ${phone} $${amt} (${st})`);
    return res.json({ ok: true, id: r.rows[0].id, status: r.rows[0].status, cid: phone, publisher_sub, amount: amt });
  } catch (err) { return res.status(500).json({ ok: false, error: err.message }); }
});

'''+ANCHOR
n=src.count(ANCHOR)
if n!=1: sys.exit("ABORT: anchor found %d times, expected 1. No change."%n)
src=src.replace(ANCHOR, ENDPOINT, 1)
shutil.copyfile(FILE, FILE+".pre-325.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 325 applied to",FILE)
