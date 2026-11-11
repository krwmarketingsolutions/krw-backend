#!/usr/bin/env python3
r"""
patch-386.py  --  buyer disposition postbacks (CAG first)

Kyler, Nov 11: CAG has its own portal and no shared sheet, so they need a
postback URL to report lead dispositions back to KRW.

New (krw-backend/server.js):
  POST or GET  /postback/buyer/:buyer        (buyer = cag)
      key                  buyer-specific secret (query ?key= or x-postback-key header)
      lead_id              the VendorLeadId KRW posted (KRW-2577), or phone
      status               the buyer's status text (Signed, Rejected, Contacted, ...)
      note                 optional reason / comment
      test=1               optional dry run: validates and matches, writes nothing

  It follows the buyer-sheet scanner's rules exactly:
    - only touches leads whose buyer is that buyer
    - Signed / Retained goes to the billable approval queue (pending Kyler's
      approval, at the buyer's payout for the lead's state); the lead reads
      Pending until approved
    - Rejected / Open / anything else updates the lead's disposition
    - an approved billable is never downgraded
  Every call is written to buyer_postback_log for the audit trail.

  GET /buyer-postbacks   (admin only)  - posting URL + recent log per buyer,
      so the Kyler0707 portal can show the instructions behind the login.

The secret is derived from the server's token secret (or <BUYER>_POSTBACK_KEY
if set), so nothing secret lives in this public repo.

Backup: server.js.pre-386.bak    Safe to re-run: refuses twice.
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
if "patch 386" in src:
    sys.exit("patch 386 already applied - nothing to do")

ANCHOR = "app.post('/buyer-sheets/scan', requireKey, async (req, res) => {"

BLOCK = r'''// ── patch 386: buyer disposition postbacks ───────────────────────────────────
// Buyers with their own system (CAG first) push lead updates here instead of
// keeping a sheet. Same rules as the sheet scanner (bsScanOne): only that
// buyer's leads, signings go to the approval queue, approved billables are
// never downgraded. Every call is logged.
const BUYER_POSTBACKS = {
  cag: { key: 'cag', label: 'CAG', buyer: 'CAG', vertical: 'MVA', amount: (st) => CAG_PAYOUT(st) },
};
function bpKey(buyerKey) {
  const env = process.env[String(buyerKey).toUpperCase() + '_POSTBACK_KEY'];
  if (env) return env;
  return _crypto288.createHmac('sha256', _tokenSecret()).update('buyer-postback|' + buyerKey).digest('hex').slice(0, 32);
}
function bpKeyOk(given, buyerKey) {
  const a = Buffer.from(String(given || '')), b = Buffer.from(bpKey(buyerKey));
  return a.length === b.length && _crypto288.timingSafeEqual(a, b);
}
(async () => {
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS buyer_postback_log (
      id SERIAL PRIMARY KEY, buyer_key TEXT, received_at TIMESTAMPTZ DEFAULT NOW(), lead_id INT,
      reported_status TEXT, reported_note TEXT, result TEXT, detail TEXT, raw JSONB)`);
  } catch (e) { console.error('[Buyer Postback] table init failed:', e.message); }
})();

async function buyerPostback(req, res) {
  const cfg = BUYER_POSTBACKS[String(req.params.buyer || '').toLowerCase()];
  if (!cfg) return res.status(404).json({ ok: false, error: 'Unknown buyer' });
  const p = Object.assign({}, req.query || {}, (req.body && typeof req.body === 'object' && !Array.isArray(req.body)) ? req.body : {});
  const norm = (k) => String(k).toLowerCase().replace(/[^a-z0-9]/g, '');
  const pick = (...keys) => {
    for (const k of Object.keys(p)) {
      if (keys.includes(norm(k))) { const v = p[k]; if (v !== undefined && v !== null && typeof v !== 'object' && String(v).trim() !== '') return String(v).trim(); }
    }
    return '';
  };
  if (!bpKeyOk(req.headers['x-postback-key'] || pick('key'), cfg.key)) return res.status(401).json({ ok: false, error: 'Invalid or missing key' });

  const ref = pick('leadid', 'vendorleadid', 'krwid', 'reference', 'referenceid');
  const refNum = (ref.match(/^(?:KRW-)?(\d{1,9})$/i) || [])[1] || null;
  const phone = bsPhone(pick('phone', 'mobile', 'phonenumber', 'callerid'));
  const statusRaw = pick('status', 'disposition', 'dispo', 'leadstatus').slice(0, 200);
  const noteRaw = pick('note', 'notes', 'reason', 'comment', 'comments').slice(0, 500);
  const flag = pick('signed', 'billable', 'retained');
  const isTest = /^(1|true|yes)$/i.test(pick('test'));
  const safe = {}; Object.keys(p).forEach(k => { if (norm(k) !== 'key') safe[k] = p[k]; });
  const log = async (leadId, result, detail) => {
    try {
      await pool.query(`INSERT INTO buyer_postback_log (buyer_key, lead_id, reported_status, reported_note, result, detail, raw) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)`,
        [cfg.key, leadId, statusRaw, noteRaw, result, detail, JSON.stringify(safe)]);
    } catch (e) { console.error('[Buyer Postback] log failed:', e.message); }
  };
  try {
    if (ref && !refNum) { await log(null, 'error', 'bad lead_id: ' + ref.slice(0, 60)); return res.status(400).json({ ok: false, error: 'lead_id must be the VendorLeadId we sent you, like KRW-2577' }); }
    if (!refNum && !phone) { await log(null, 'error', 'no lead reference'); return res.status(400).json({ ok: false, error: 'Send lead_id (the VendorLeadId we sent you, like KRW-2577) or phone' }); }
    const cls = bsClassify(cfg, { status: statusRaw, notes: noteRaw, billableFlag: flag });
    if (!cls) { await log(null, 'error', 'no status'); return res.status(400).json({ ok: false, error: 'status is required' }); }

    const lead = (await pool.query(
      `SELECT id, status, billable, raw, publisher_sub, state, phone FROM leads
       WHERE ( ($1::int IS NOT NULL AND id=$1::int)
            OR ($1::int IS NULL AND $2::text IS NOT NULL AND RIGHT(regexp_replace(phone,'\\D','','g'),10)=$2::text) )
         AND raw->>'buyer_name' = $3::text
       ORDER BY received_at DESC LIMIT 1`, [refNum ? parseInt(refNum, 10) : null, phone, cfg.buyer])).rows[0];
    if (!lead) { await log(null, 'unmatched', (ref || phone || '').slice(0, 60)); return res.status(404).json({ ok: false, error: 'No lead with that reference was sent to ' + cfg.label }); }
    if (refNum && phone && bsPhone(lead.phone) && bsPhone(lead.phone) !== phone) {
      await log(lead.id, 'error', 'phone does not match lead_id');
      return res.status(409).json({ ok: false, error: 'phone does not match that lead_id' });
    }
    const leadRef = 'KRW-' + lead.id;
    if (isTest) { await log(lead.id, 'test', cls.status); return res.json({ ok: true, test: true, lead_id: leadRef, would_record: cls.status }); }

    const raw = lead.raw || {};
    const locked = raw.billable_locked === true || raw.billable_locked === 'true';
    if (cls.kind === 'billable') {
      const dup = await pool.query(`SELECT id FROM billable_queue WHERE lead_id=$1 LIMIT 1`, [lead.id]);
      if (dup.rows.length || lead.billable) { await log(lead.id, 'duplicate', 'already signed or queued'); return res.json({ ok: true, lead_id: leadRef, recorded: 'Signed', already_on_file: true }); }
      await pool.query(
        `INSERT INTO billable_queue (cid, amount, publisher_sub, lead_id, status, raw) VALUES ($1,$2,$3,$4,'pending',$5::jsonb)`,
        [bsPhone(lead.phone) || String(lead.id), cfg.amount(lead.state), lead.publisher_sub, lead.id,
         JSON.stringify({ source: 'buyer_postback', buyer_key: cfg.key, buyer: cfg.buyer, vertical: cfg.vertical, sheet_status: statusRaw, sheet_notes: noteRaw, scanned_at: new Date().toISOString(), trigger: 'postback' })]);
      // Same as the sheet scanner: nothing reads Signed until Kyler approves it.
      await pool.query(
        `UPDATE leads SET buyer_status='Pending', notes='In outreach', status=CASE WHEN status='buyer_rejected' THEN 'forwarded' ELSE status END,
           raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('buyer_disposition', jsonb_build_object('source','buyer_postback','buyer_key',$1::text,'status','Pending','note','In outreach','sheet_status',$2::text,'awaiting_approval',true,'synced_at',NOW(),'confirmed_at',NOW()))
         WHERE id=$3::int`, [cfg.key, statusRaw, lead.id]);
      await log(lead.id, 'queued', 'signed - awaiting approval');
      console.log(`[Buyer Postback] $ ${cfg.label} | lead ${lead.id} | ${statusRaw} -> queued for approval`);
      return res.json({ ok: true, lead_id: leadRef, recorded: 'Signed' });
    }
    if (locked || lead.billable) { await log(lead.id, 'skipped', 'lead already billable - not downgraded'); return res.json({ ok: true, lead_id: leadRef, recorded: cls.status, note: 'lead is already marked signed' }); }
    let note = cls.note;
    if (/\bcag\b|law\s*firm|lawfirm/i.test(note)) note = cls.status;   // never leak a buyer name into a publisher-facing note
    await pool.query(
      `UPDATE leads SET buyer_status=$1::text,
         status=CASE WHEN $2::text='rejected' THEN 'buyer_rejected' WHEN status='buyer_rejected' THEN 'forwarded' ELSE status END, notes=$3::text,
         raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('buyer_disposition', jsonb_build_object('source','buyer_postback','buyer_key',$4::text,'status',$1::text,'note',$3::text,'sheet_status',$5::text,'synced_at',NOW(),'confirmed_at',NOW()))
       WHERE id=$6::int`, [cls.status, cls.kind, note, cfg.key, statusRaw, lead.id]);
    await log(lead.id, 'updated', cls.status);
    console.log(`[Buyer Postback] ${cfg.label} | lead ${lead.id} | ${statusRaw} -> ${cls.status}`);
    return res.json({ ok: true, lead_id: leadRef, recorded: cls.status });
  } catch (err) {
    await log(null, 'error', err.message.slice(0, 200));
    return res.status(500).json({ ok: false, error: 'Server error - please retry' });
  }
}
app.all('/postback/buyer/:buyer', express.urlencoded({ extended: false }), buyerPostback);

// Admin only: the posting URL (with its key) and the recent log per buyer.
app.get('/buyer-postbacks', requireKey, async (req, res) => {
  try {
    const host = 'https://' + req.get('host');
    const out = [];
    for (const k of Object.keys(BUYER_POSTBACKS)) {
      const cfg = BUYER_POSTBACKS[k];
      const logq = await pool.query(`SELECT received_at, lead_id, reported_status, reported_note, result, detail FROM buyer_postback_log WHERE buyer_key=$1 ORDER BY id DESC LIMIT 25`, [k]);
      const tot = await pool.query(`SELECT COUNT(*)::int AS n, MAX(received_at) AS last FROM buyer_postback_log WHERE buyer_key=$1 AND result IN ('updated','queued','duplicate','skipped')`, [k]);
      out.push({ key: k, label: cfg.label, url: host + '/postback/buyer/' + k, secret: bpKey(k), received: tot.rows[0].n, last_received: tot.rows[0].last, log: logq.rows });
    }
    res.json({ ok: true, buyers: out });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
// ── end patch 386 ────────────────────────────────────────────────────────────

'''

if src.count(ANCHOR) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1." % src.count(ANCHOR))
src = src.replace(ANCHOR, BLOCK + ANCHOR, 1)
shutil.copyfile(FILE, FILE + ".pre-386.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 386 applied")
