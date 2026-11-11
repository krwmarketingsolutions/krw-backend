#!/usr/bin/env python3
r"""
patch-387.py  --  aged leads resent to a new buyer start over with that buyer

Kyler, Nov 11: the 287 aged leads resent to CAG on Nov 10 still show their OLD
buyer's disposition (NLD's rejection, or "no buyer update in 30+ days") as if
CAG had reported it. CAG has not reported on them yet.

Two causes:
  a. /leads/forward-to-cag stamped the lead as CAG's but left the previous
     buyer's disposition on the record.
  b. The 30-day auto write-off counts from the day the lead first came in, so
     the next janitor run wrote the resent leads straight back off.

Edits (krw-backend/server.js):
  1. Auto write-off clock starts at the resend (manual_forward_at) when there
     is one, otherwise at received_at as before.
  2. /leads/forward-to-cag: on accept, the old disposition moves to
     raw.previous_disposition (+ previous_buyer), aged_out is cleared, and
     CAG's accept response is saved on the lead.
  3. /leads/feed: adds prev_buyer, prev_note, resent_at (stripped for the
     publisher portal role along with the other buyer fields).
  4. POST /admin/aged-resend/fix  { buyer:'CAG', apply:false|true } (admin)
     one-time repair for leads already resent: dry run by default. Skips
     billable leads, leads with revenue on file, signings awaiting approval,
     and any lead whose disposition was set after the resend.

Backup: server.js.pre-387.bak    Safe to re-run: refuses twice.
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
if "patch 387" in src:
    sys.exit("patch 387 already applied - nothing to do")

ADMIN_ANCHOR = "app.post('/buyer-sheets/scan', requireKey, async (req, res) => {"
ADMIN_BLOCK = r'''// ── patch 387: aged leads resent to a new buyer start over with that buyer ───
const AR_BUYER_BY_KEY = { 'nld-mva': 'NLD CPA', 'lt-intake': 'LT-Intake', 'ch-intake': 'CH-Intake', 'dt-intake': 'DT-Intake', 'mva-003': 'MVA-003-LT', 'lag-mva': 'Legal Advocate Group', 'cag': 'CAG' };
function agedResendReset(row, newBuyer) {
  const raw = row.raw || {}, d = raw.buyer_disposition || null, br = row.buyer_response || {};
  let prev = raw.buyer_name && raw.buyer_name !== newBuyer ? raw.buyer_name : null;
  if (!prev && d && AR_BUYER_BY_KEY[d.buyer_key] && AR_BUYER_BY_KEY[d.buyer_key] !== newBuyer) prev = AR_BUYER_BY_KEY[d.buyer_key];
  if (!prev && Array.isArray(br.attempts)) { const a = br.attempts.filter(x => x && x.accepted && x.buyer && x.buyer !== newBuyer).pop(); if (a) prev = a.buyer; }
  if (!prev && br.nld_attempted && br.nld_response && /accepted/i.test(String(br.nld_response.status || ''))) prev = 'NLD CPA';
  if (!prev && raw.manual_cert_send && raw.manual_cert_send.buyer && raw.manual_cert_send.buyer !== newBuyer) prev = raw.manual_cert_send.buyer;
  if (!prev && raw.previous_buyer && raw.previous_buyer !== newBuyer) prev = raw.previous_buyer;
  const out = { aged_resend: true };
  if (prev) out.previous_buyer = prev;
  if (d) out.previous_disposition = d;
  return out;
}
app.post('/admin/aged-resend/fix', requireKey, async (req, res) => {
  try {
    const buyer = String((req.body && req.body.buyer) || 'CAG');
    const apply = !!(req.body && req.body.apply === true);
    const q = await pool.query(
      `SELECT id, status, buyer_status, billable, revenue, raw, buyer_response FROM leads
       WHERE raw->>'buyer_name' = $1 AND raw->>'manual_forward' = 'true' ORDER BY id`, [buyer]);
    const rep = { ok: true, buyer, apply, total: q.rows.length, fixed: 0, already_ok: 0, skipped: [], by_previous_buyer: {}, by_old_disposition: {} };
    for (const row of q.rows) {
      const raw = row.raw || {}, d = raw.buyer_disposition || null;
      const fwdAt = Date.parse(raw.manual_forward_at || '') || 0;
      const skip = (why) => rep.skipped.push({ id: row.id, why });
      if (row.billable === true || raw.billable_locked === true || raw.billable_locked === 'true') { skip('billable'); continue; }
      if (Number(row.revenue) > 0) { skip('has revenue on file'); continue; }
      if (d && d.awaiting_approval) { skip('signing awaiting approval'); continue; }
      const dAt = d ? (Date.parse(d.synced_at || '') || 0) : 0;
      const stale = d && (d.source === 'aged_out' || (d.source !== 'buyer_postback' && d.buyer_key !== 'cag' && dAt < fwdAt));
      if (d && !stale) { skip('disposition set after the resend (' + (d.source || '?') + ')'); continue; }
      if (!d && row.status === 'forwarded' && raw.aged_out !== true) { rep.already_ok++; continue; }
      const reset = agedResendReset(row, buyer);
      const pk = reset.previous_buyer || 'unknown'; rep.by_previous_buyer[pk] = (rep.by_previous_buyer[pk] || 0) + 1;
      const dk = d ? (d.source === 'aged_out' ? 'aged out' : (d.status || '?')) : 'none'; rep.by_old_disposition[dk] = (rep.by_old_disposition[dk] || 0) + 1;
      if (apply) {
        await pool.query(
          `UPDATE leads SET status='forwarded', buyer_status='Accepted', buyer_error=NULL, notes=NULL,
             raw = (COALESCE(raw,'{}'::jsonb) - 'buyer_disposition' - 'aged_out') || $1::jsonb WHERE id=$2`,
          [JSON.stringify(reset), row.id]);
      }
      rep.fixed++;
    }
    if (apply) console.log(`[Aged Resend] ${buyer}: reset ${rep.fixed} lead(s) to awaiting, ${rep.skipped.length} skipped`);
    res.json(rep);
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
// ── end patch 387 ────────────────────────────────────────────────────────────

'''

edits = [
# 1. write-off clock starts at the resend
("        AND received_at < NOW() - make_interval(days => ${JAN_AGEOUT_DAYS})",
 "        AND GREATEST(received_at, COALESCE(NULLIF(raw->>'manual_forward_at','')::timestamptz, received_at)) < NOW() - make_interval(days => ${JAN_AGEOUT_DAYS})   -- patch 387: a resend restarts the clock"),

# 2. forward-to-cag resets the lead for the new buyer
("""      await pool.query(`UPDATE leads SET status='forwarded', buyer_status='Accepted', buyer_error=NULL,
          raw = COALESCE(raw,'{}'::jsonb) || $1::jsonb WHERE id=$2`,
        [JSON.stringify({ buyer_name: 'CAG', manual_forward: true, manual_forward_at: new Date().toISOString() }), row.id]);""",
 """      // patch 387: the lead starts over with CAG - the old buyer's disposition
      // moves to previous_disposition, and CAG's accept is kept on the record.
      const nowIso = new Date().toISOString();
      await pool.query(`UPDATE leads SET status='forwarded', buyer_status='Accepted', buyer_error=NULL, notes=NULL,
          buyer_response = COALESCE(buyer_response,'{}'::jsonb) || $3::jsonb,
          raw = (COALESCE(raw,'{}'::jsonb) - 'buyer_disposition' - 'aged_out') || $1::jsonb WHERE id=$2`,
        [JSON.stringify(Object.assign(agedResendReset(row, 'CAG'), { buyer_name: 'CAG', manual_forward: true, manual_forward_at: nowIso })), row.id,
         JSON.stringify({ manual_forward: Object.assign({ buyer: 'CAG', at: nowIso }, r.result || {}) })]);"""),

# 3a. feed fields
("""              raw->'buyer_disposition'->>'source'       as disp_source
       FROM leads""",
 """              raw->'buyer_disposition'->>'source'       as disp_source,
              raw->>'previous_buyer' as prev_buyer, raw->'previous_disposition'->>'note' as prev_note, raw->>'manual_forward_at' as resent_at   -- patch 387
       FROM leads"""),

# 3b. publisher portal role never sees them
("""  const drop = ["buyer_name", "buyer_status", "buyer_error", "buyer_intake_id", "revenue", "billable", "notes", "zapier_status"];""",
 """  const drop = ["buyer_name", "buyer_status", "buyer_error", "buyer_intake_id", "revenue", "billable", "notes", "zapier_status", "prev_buyer", "prev_note", "resent_at", "disp_source"];   // patch 387"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
if src.count(ADMIN_ANCHOR)!=1: sys.exit("ABORT: admin anchor found %d times." % src.count(ADMIN_ANCHOR))
for a,r in edits: src=src.replace(a,r,1)
src = src.replace(ADMIN_ANCHOR, ADMIN_BLOCK + ADMIN_ANCHOR, 1)
shutil.copyfile(FILE, FILE+".pre-387.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 387 applied")
