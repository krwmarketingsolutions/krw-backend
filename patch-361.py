#!/usr/bin/env python3
r"""
patch-361.py  --  admin endpoint to post/backfill a Joshua SSDI "Signed" deal

Joshua's SSDI Signed line (KRW-JOSHUA-SIGNED) lives in the calls table and is
normally created by the Trackdrive / j-signed postback when a call comes
through, then dispositioned. When a signed transfer never produced a call
record on our side (the postback didn't fire), there is nothing to disposition,
so the deal is missing from Joshua's portal and can't be added through any
admin-authed route - PATCH /calls/:id/billable only updates an existing call,
and /calls/update needs an ingestion key + DID and fires the daily-report email.

This adds POST /calls/joshua-signed/upsert (admin bearer, requireKey): upsert a
KRW-JOSHUA-SIGNED signed deal by phone - update the existing call if one is
there, otherwise insert one that matches the shape of the real postback records
(source_system j_signed_postback so it shows on /calls/feed; campaign
ssdi-signed-td; vertical SSDI; call_status_label cpa). Defaults: payout $200
(SSDI CPA rate), disposition "Retained — Initial Application", billable true.
Idempotent, no email side effect.

  krw-backend/server.js (one inserted route).

Backup: server.js.pre-361.bak    Safe to re-run: refuses twice.
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
if "patch 361" in src or "/calls/joshua-signed/upsert" in src:
    sys.exit("patch 361 already applied - nothing to do")

anchor = "app.post('/send-invoice', requireKey, async (req, res) => {"
if src.count(anchor) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % src.count(anchor))

route = r"""// patch 361: admin upsert for a Joshua SSDI "Signed" deal (KRW-JOSHUA-SIGNED).
// Fills the gap where a signed transfer produced no call record on our side.
// Upsert by phone; matches the real j_signed_postback record shape so it shows
// on /calls/feed. Default payout $200 (SSDI CPA), disposition Retained.
app.post('/calls/joshua-signed/upsert', requireKey, async (req, res) => {
  const b = req.body || {};
  let phone = String(b.phone || b.caller_id || b.cid || '').replace(/\D/g, '');
  if (phone.length === 11 && phone.startsWith('1')) phone = phone.slice(1);
  if (!phone) return res.status(400).json({ ok: false, error: 'phone (or caller_id) required' });
  const payout   = (b.payout_amount !== undefined && b.payout_amount !== null && b.payout_amount !== '')
                     ? parseFloat(b.payout_amount) : 200;
  if (!isFinite(payout) || payout < 0) return res.status(400).json({ ok: false, error: 'payout_amount must be a non-negative number' });
  const billable = (b.billable !== false);
  const dispo    = b.disposition || 'Retained — Initial Application';
  const callDate = b.call_date || new Date().toISOString().slice(0, 10);
  try {
    const upd = await pool.query(
      `UPDATE calls SET billable = $1, payout_amount = $2::numeric, call_status_label = 'cpa',
         disposition = $3, caller_name = COALESCE($4, caller_name)
       WHERE publisher_sub = 'KRW-JOSHUA-SIGNED'
         AND regexp_replace(caller_id, '\D', '', 'g') = $5
       RETURNING id`,
      [billable, payout.toFixed(2), dispo, b.caller_name || null, phone]);
    if (upd.rows.length) {
      console.log(`[J-Signed Upsert] updated call ${upd.rows[0].id} | ${phone} | $${payout} | ${dispo}`);
      return res.json({ ok: true, action: 'updated', id: upd.rows[0].id, phone, payout_amount: payout.toFixed(2) });
    }
    const ins = await pool.query(
      `INSERT INTO calls
         (call_date, caller_id, caller_name, billable, publisher_sub, vertical,
          campaign, campaign_name, buyer_name, payout_amount, disposition,
          call_status, call_status_label, source_system, raw, received_at)
       VALUES ($1,$2,$3,$4,'KRW-JOSHUA-SIGNED','SSDI',
               'ssdi-signed-td','SSDI Signed (TD)','J-Signed Buyer',$5::numeric,$6,
               'Completed','cpa','j_signed_postback',$7::jsonb,NOW())
       RETURNING id`,
      [callDate, phone, b.caller_name || null, billable, payout.toFixed(2), dispo,
       JSON.stringify({ source: 'admin_backfill', ref: b.ref || null, entered_by: 'admin', entered_at: new Date().toISOString() })]);
    console.log(`[J-Signed Upsert] inserted call ${ins.rows[0].id} | ${phone} | $${payout} | ${dispo}`);
    return res.json({ ok: true, action: 'inserted', id: ins.rows[0].id, phone, payout_amount: payout.toFixed(2) });
  } catch (err) { return res.status(500).json({ ok: false, error: err.message }); }
});

"""

src = src.replace(anchor, route + anchor, 1)
shutil.copyfile(FILE, FILE + ".pre-361.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 361 applied to", FILE)
