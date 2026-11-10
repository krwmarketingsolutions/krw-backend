#!/usr/bin/env python3
r"""
patch-369.py  --  add CAG Advertising (Dylan Rossi) as an MVA buyer (posting)

Kyler (Nov 10) forwarded CAG's posting docs. CAG is an MVA buyer (Dylan Rossi /
Tom intro). This wires up delivery of KRW MVA leads to CAG's post endpoint.

Posting spec (from CAG's doc):
  POST https://sys.cagadv.com/leadPost.php?cid=50&fid=3234  (form-urlencoded)
  required: fname, lname, phone, state, email, IPAddress, AutoAccident(Y/Yes)
  optional: mobile, address1, address2, city, zip, Comments, SubId, SubId2,
            clickid, t_id, VendorLeadId, initial
  states: NY, NJ, PA, AZ, IA, MT, NE, NV, ND, OK, UT, WY, TX (13)

Per-state CPR payout (what CAG pays KRW per retained case):
  NY/NJ/PA $2,500 · TX $2,250 · AZ/IA/MT/NE/NV/ND/OK/UT/WY $1,800
Stored in CAG_PAYOUT() so it is ready for billing. NOTE: how KRW learns which
leads CAG retained (disposition/billing read-back) is NOT defined yet - that is
the Nov call - so, exactly like every other buyer, route-time revenue stays 0
and billing is wired later.

Routing: CAG is added at priority 3 on the two mva-nyc-split ladders (NYC /
Leadbloom) and the LA-HI mva-intake line, BELOW NLD (priority 1) and LAG
(priority 2). So NLD keeps first dibs everywhere it buys (the maximize-NLD
rule), LAG stays where it is, and CAG is the sole buyer of its new states
(NY/NJ/PA) and the fallback that catches NLD/LAG overflow in the shared states.

STAGED OFF: CAG_LIVE defaults false, so CAG is wired but NOT yet selected by the
ladder. Set CAG_LIVE=true on Railway to go live once the post format is
confirmed. The test route POST /leads/forward-to-cag ({test:true,...} or
{krw_id}) works regardless of the flag and is how we verify CAG's response
format before flipping it on; the accept/reject parse is conservative until
that real response is seen.

  krw-backend/server.js (six edits).

Backup: server.js.pre-369.bak    Safe to re-run: refuses twice.
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
if "patch 369" in src:
    sys.exit("patch 369 already applied - nothing to do")

edits = []

# ── A: CAG helpers + sender + test route (inserted after lagAppendRow) ────────
CAG_BLOCK = r"""
  if (r.status >= 300) throw new Error('append ' + r.status + ': ' + String(r.body).slice(0, 160));
  return JSON.parse(r.body || '{}');
}

// ── patch 369: CAG Advertising (Dylan Rossi) MVA buyer - lead posting ─────────
// Posts KRW MVA leads to CAG's endpoint. Billing (which leads CAG retained) is
// set up after the Nov call; CAG_PAYOUT holds the agreed per-state CPR price for
// then. Staged off (CAG_LIVE=true on Railway to route live). Response parse is
// conservative until CAG's exact accept/reject format is confirmed by a test.
const CAG_POST_URL = process.env.CAG_POST_URL || 'https://sys.cagadv.com/leadPost.php?cid=50&fid=3234';
const CAG_STATES   = ['NY','NJ','PA','AZ','IA','MT','NE','NV','ND','OK','UT','WY','TX'];
function CAG_LIVE()  { return (process.env.CAG_LIVE || 'false') !== 'false'; }   // default OFF until verified
function CAG_PAYOUT(st) {
  st = String(st || '').toUpperCase().trim();
  if (st === 'NY' || st === 'NJ' || st === 'PA') return 2500;
  if (st === 'TX') return 2250;
  if (CAG_STATES.includes(st)) return 1800;
  return 0;
}

// POST the lead to CAG as application/x-www-form-urlencoded (cid/fid stay in URL).
function cagPostForm(params) {
  return new Promise((resolve, reject) => {
    const https = require('https');
    const url = new URL(CAG_POST_URL);
    const body = new URLSearchParams(params).toString();
    const options = { hostname: url.hostname, path: url.pathname + (url.search || ''), method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) } };
    const rq = https.request(options, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => resolve({ status: r.statusCode, body: d })); });
    rq.on('error', reject); rq.write(body); rq.end();
  });
}

async function sendToCag(b, leadState, leadId) {
  const digits = String(b.phone || '').replace(/\D/g, '');
  const params = {
    fname: b.first_name || '', lname: b.last_name || '',
    phone: digits, mobile: digits,
    state: String(leadState || '').toUpperCase().trim(),
    email: b.email || '',
    IPAddress: b.ip_address || b.ipaddress || b.IPAddress || '',
    AutoAccident: 'Yes',
    address1: b.address || b.address1 || '',
    city: b.city || '',
    zip: b.zip_code || b.zip || '',
    Comments: b.summary || b.injury || b.physical_injury || '',
    VendorLeadId: 'KRW-' + (leadId == null ? 'TEST' : leadId),
    SubId: b.publisher_sub || '',
  };
  const r = await cagPostForm(params);
  const bodyText = String(r.body || '');
  const low = bodyText.toLowerCase();
  const http2xx = r.status >= 200 && r.status < 300;
  const looksRejected = /reject|declin|duplicat|\bdupe\b|invalid|\berror\b|\bfail|not\s*accept/.test(low);
  const looksAccepted = /accept|success|received|"?status"?\s*[:=]\s*"?(1|accepted|success)/.test(low);
  // Until CAG's real response is seen: accept on 2xx with no rejection signal.
  const accepted = http2xx && !looksRejected && (looksAccepted || true);
  console.log(`[CAG] ${accepted ? 'OK' : 'X'} ${b.first_name} ${b.last_name} | ${params.state} | http ${r.status} | ${bodyText.slice(0, 160)}`);
  return { accepted, result: { status: accepted ? 'success' : 'rejected', http: r.status, message: bodyText.slice(0, 300) }, sent: params };
}

// Manual / test: { test:true, phone?, state? } posts a clearly-marked fake lead
// so CAG's exact response can be seen; { krw_id } re-sends a stored lead. Works
// even when CAG_LIVE is off. Never touches a billable lead.
app.post('/leads/forward-to-cag', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [process.env.API_KEY || '64tgzb5ostadx1azjio9crdlduw4vf29', process.env.LEAD_API_KEY || 'krwleads2026secure'];
  const bearerOk = typeof callerRole === 'function' && callerRole(req) === 'admin';
  if (!validKeys.includes(key) && !bearerOk) return res.status(401).json({ ok: false, error: 'Unauthorized' });
  const { krw_id, test, phone, state } = req.body || {};
  try {
    if (test === true) {
      const b = { first_name: 'Test', last_name: 'KRW-TEST', phone: String(phone || '5550001234').replace(/\D/g, ''),
        email: 'test@krwmarketingsolutions.com', ip_address: '8.8.8.8', address: '123 Test St', city: 'Phoenix', zip_code: '85001',
        summary: 'TEST LEAD - KRW integration check, please disregard', publisher_sub: 'KRW-TEST' };
      const r = await sendToCag(b, (state || 'AZ'), null);
      return res.json({ ok: r.accepted, test: true, sent: r.sent, response: r.result });
    }
    if (!krw_id) return res.status(400).json({ ok: false, error: 'krw_id required (or { test:true, phone, state })' });
    const q = await pool.query('SELECT * FROM leads WHERE id=$1', [krw_id]);
    if (!q.rows[0]) return res.status(404).json({ ok: false, error: 'Lead not found' });
    const row = q.rows[0];
    if (row.billable === true) return res.status(409).json({ ok: false, error: 'Lead is billable - refusing to touch it' });
    const b = Object.assign({}, row.raw || {}); b.phone = b.phone || row.phone; b.first_name = b.first_name || row.first_name; b.last_name = b.last_name || row.last_name; b.email = b.email || row.email;
    const st = (row.state || b.state || '').toUpperCase();
    const r = await sendToCag(b, st, row.id);
    if (r.accepted) {
      await pool.query(`UPDATE leads SET status='forwarded', buyer_status='Accepted', buyer_error=NULL,
          raw = COALESCE(raw,'{}'::jsonb) || $1::jsonb WHERE id=$2`,
        [JSON.stringify({ buyer_name: 'CAG', manual_forward: true, manual_forward_at: new Date().toISOString() }), row.id]);
    }
    return res.json({ ok: r.accepted, krw_id: row.id, sent: r.sent, response: r.result });
  } catch (e) {
    return res.status(502).json({ ok: false, error: e.message });
  }
});
"""
edits.append((
"  if (r.status >= 300) throw new Error('append ' + r.status + ': ' + String(r.body).slice(0, 160));\n"
"  return JSON.parse(r.body || '{}');\n"
"}",
CAG_BLOCK.rstrip("\n")))

# ── B: CAG rung on NYC_LADDER (priority 3, below NLD + LAG) ───────────────────
edits.append((
"    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD - sole buyer in GA/TX/SC, catches NLD's overflow in the shared states\n"
"    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1700, enabled: false,       states: 'ALL' },   // patch 305: 003 off",
"    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD - sole buyer in GA/TX/SC, catches NLD's overflow in the shared states\n"
"    { name: 'CAG',                  priority: 3, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 369: CAG 3rd - sole buyer of NY/NJ/PA, catches NLD/LAG overflow in shared states\n"
"    { name: 'MVA-003-LT',           priority: 4, group: '003', cap: 5,    payout: 1700, enabled: false,       states: 'ALL' },   // patch 305: 003 off"))

# ── C: CAG rung on LB_ONLY_LADDER ────────────────────────────────────────────
edits.append((
"    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD\n"
"  ];   // patch 368: DT-Intake removed",
"    { name: 'Legal Advocate Group', priority: 2, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 368: LAG 2nd after NLD\n"
"    { name: 'CAG',                  priority: 3, group: 'cag', cap: null, payout: CAG_PAYOUT(leadState), enabled: CAG_LIVE(), states: CAG_STATES },   // patch 369\n"
"  ];   // patch 368: DT-Intake removed"))

# ── D: CAG sender on /leads/mva-nyc-split ────────────────────────────────────
edits.append((
"    'Legal Advocate Group': async () => {   // patch 367: deliver to LAG's Google Sheet (Leads tab)\n"
"      if (!LAG_LIVE()) return { result: { status: 'paused', message: 'LAG delivery paused' }, accepted: false };\n"
"      try {\n"
"        const out = await lagAppendRow(b, leadState, leadId);\n"
"        return { result: { status: 'success', message: 'Delivered to Legal Advocate Group sheet', range: out && out.updates && out.updates.updatedRange }, accepted: true };\n"
"      } catch (e) {\n"
"        return { result: { status: 'error', message: 'LAG sheet append failed: ' + e.message }, accepted: false };\n"
"      }\n"
"    },\n"
"  };",
"    'Legal Advocate Group': async () => {   // patch 367: deliver to LAG's Google Sheet (Leads tab)\n"
"      if (!LAG_LIVE()) return { result: { status: 'paused', message: 'LAG delivery paused' }, accepted: false };\n"
"      try {\n"
"        const out = await lagAppendRow(b, leadState, leadId);\n"
"        return { result: { status: 'success', message: 'Delivered to Legal Advocate Group sheet', range: out && out.updates && out.updates.updatedRange }, accepted: true };\n"
"      } catch (e) {\n"
"        return { result: { status: 'error', message: 'LAG sheet append failed: ' + e.message }, accepted: false };\n"
"      }\n"
"    },\n"
"    'CAG': async () => {   // patch 369\n"
"      try { const r = await sendToCag(b, leadState, leadId); return { result: r.result, accepted: r.accepted }; }\n"
"      catch (e) { return { result: { status: 'error', message: 'CAG post failed: ' + e.message }, accepted: false }; }\n"
"    },\n"
"  };"))

# ── E: CAG sender on /leads/mva-intake ───────────────────────────────────────
edits.append((
"    'DT-Intake': async () => { const r = await sendToDtIntake(b, leadState, leadId, MVA_INTAKE_PUB); return { result: r.result, accepted: r.accepted }; },   // patch 350",
"    'DT-Intake': async () => { const r = await sendToDtIntake(b, leadState, leadId, MVA_INTAKE_PUB); return { result: r.result, accepted: r.accepted }; },   // patch 350\n"
"    'CAG': async () => { try { const r = await sendToCag(b, leadState, leadId); return { result: r.result, accepted: r.accepted }; } catch (e) { return { result: { status: 'error', message: 'CAG post failed: ' + e.message }, accepted: false }; } },   // patch 369"))

# ── F: CAG rung on the /leads/mva-intake ladder ──────────────────────────────
edits.append((
"  const ladder = [\n"
"      { name: 'Legal Advocate Group', enabled: LAG_LIVE() && LAG_STATES.includes(leadState) },   // patch 367 / 368: LAG only (DT-Intake removed)\n"
"    ]",
"  const ladder = [\n"
"      { name: 'Legal Advocate Group', enabled: LAG_LIVE() && LAG_STATES.includes(leadState) },   // patch 367 / 368: LAG only (DT-Intake removed)\n"
"      { name: 'CAG', enabled: CAG_LIVE() && CAG_STATES.includes(leadState) },   // patch 369\n"
"    ]"))

for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit %d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-369.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 369 applied to", FILE)
