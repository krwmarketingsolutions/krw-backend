#!/usr/bin/env python3
"""
patch-324.py  --  attach a TrustedForm cert to a rejected lead and forward it
                  to a chosen buyer (for when the cert arrives after the lead)

Leads submitted without a TrustedForm cert are rejected by the patch-270 gate
with "Missing or invalid TrustedForm certificate". When the cert comes in
later, this endpoint attaches it and forwards the lead to exactly one buyer,
flipping it to forwarded on accept. No ladder, no second buyer - you pick the
buyer, so e.g. an AZ lead can be sent to NLD and another to LT-Intake only.

  POST /leads/:id/attach-cert-and-send   (admin key)
    body { cert: "https://cert.trustedform.com/...", buyer: "NLD CPA" | "LT-Intake" | "CH-Intake" | "MVA-003-LT" }

Guards: refuses if already forwarded or billable, MVA only, CA/CO blocked,
cert must be a trustedform URL, buyer must be known. Reuses janSend().

Backup: server.js.pre-324.bak    Safe to re-run: refuses twice.
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
src = open(FILE, encoding="utf-8").read()
if "attach-cert-and-send" in src or "patch 324" in src:
    sys.exit("patch 324 already applied to server.js - nothing to do")
if "async function janSend" not in src:
    sys.exit("janSend not found - unexpected server.js")

ANCHOR = "});\n// ─── end patch 272 ────────────────────────────────────────────────────────────"
ENDPOINT = r'''});

// ─── patch 324: attach a late TrustedForm cert and forward to one buyer ───────
app.post('/leads/:id/attach-cert-and-send', requireKey, async (req, res) => {
  try {
    const { cert, buyer } = req.body || {};
    const ALLOWED = ['NLD CPA', 'LT-Intake', 'CH-Intake', 'MVA-003-LT'];
    if (!cert || !/^https:\/\/cert\.trustedform\.com\/[a-z0-9]+/i.test(String(cert)))
      return res.status(400).json({ ok: false, error: 'a valid trustedform_cert_url is required' });
    if (!ALLOWED.includes(buyer))
      return res.status(400).json({ ok: false, error: 'buyer must be one of ' + ALLOWED.join(', ') });
    const q = await pool.query('SELECT * FROM leads WHERE id=$1', [req.params.id]);
    if (!q.rows[0]) return res.status(404).json({ ok: false, error: 'Lead not found' });
    const row = q.rows[0];
    if (row.status === 'forwarded') return res.status(409).json({ ok: false, error: 'Lead is already forwarded - refusing to double-send' });
    if (row.billable === true)      return res.status(409).json({ ok: false, error: 'Lead is billable - refusing to touch it' });
    if ((row.vertical || '') !== 'MVA') return res.status(400).json({ ok: false, error: 'Only MVA leads can be sent here' });
    const st = janState(row.state);
    if (st === 'CA' || st === 'CO') return res.status(400).json({ ok: false, error: st + ' is blocked company-wide' });

    // attach the cert to the stored lead first, so it is recorded even if the buyer declines
    await pool.query(
      `UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('trustedform_cert_url',$1::text,'trusted_form_cert_url',$1::text) WHERE id=$2`,
      [String(cert), row.id]);
    const b = Object.assign({}, row.raw || {}, {
      first_name: row.first_name, last_name: row.last_name, phone: row.phone, email: row.email,
      state: st, trustedform_cert_url: String(cert), trusted_form_cert_url: String(cert) });

    let out;
    try { out = await janSend(buyer, b, st, row.id, row.publisher_sub || ''); }
    catch (e) { out = { result: { error: e.message }, accepted: false }; }
    console.log(`[AttachCertSend] ${out.accepted ? '✓' : '✕'} ${buyer} | lead ${row.id} | ${st}`);

    if (out.accepted) {
      await pool.query(
        `UPDATE leads SET status='forwarded', buyer_status='Accepted', buyer_error=NULL,
           buyer_response = COALESCE(buyer_response,'{}'::jsonb) || $1::jsonb,
           raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('buyer_name',$2::text,'manual_cert_send', jsonb_build_object('at', NOW(), 'buyer', $2::text)) WHERE id=$3`,
        [JSON.stringify({ manual_cert_send: out.result }), buyer, row.id]);
      return res.json({ ok: true, result: 'success', buyer, krw_id: row.id, response: out.result });
    }
    await pool.query(
      `UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('manual_cert_send', jsonb_build_object('at', NOW(), 'buyer', $1::text, 'note', 'buyer did not accept')) WHERE id=$2`,
      [buyer, row.id]);
    return res.json({ ok: false, result: 'rejected', buyer, krw_id: row.id, response: out.result });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});
// ─── end patch 272 ────────────────────────────────────────────────────────────'''

n = src.count(ANCHOR)
if n != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % n)
src = src.replace(ANCHOR, ENDPOINT, 1)
shutil.copyfile(FILE, FILE + ".pre-324.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 324 applied to", FILE)
