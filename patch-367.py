#!/usr/bin/env python3
r"""
patch-367.py  --  add Legal Advocate Group (LAG) as a live MVA buyer

Kyler (Nov 3): stand LAG up as a buyer and grant them the MVA states NLD is
NOT currently buying. Of LAG's seven requested states (AZ, NV, GA, TX, NM, SC,
UT), NLD already buys AZ/NV/NM/UT, so LAG gets the rest: GA, TX, SC, at $2,200
per signed case. Their leads flow to the Google Sheet we built for them
(KRW x Legal Advocate Group - MVA Leads), and their dispositions are read back
from that sheet's "Dispositions" tab for billing.

How it works:
  * DELIVERY (new): a LAG-matched lead is delivered by APPENDING a row to the
    sheet's "Leads" tab. This is a brand-new write path. The existing
    buyer-sheet scanner is read-only (spreadsheets.readonly); this adds a
    SEPARATE write-scoped service-account token (lagWriteToken) and an append
    helper (lagAppendRow) so the read-only scanner token is left completely
    untouched. The sheet is shared with the service account as editor.
  * ROUTING: LAG is added as a buyer rung (priority 1, states GA/TX/SC) on the
    two live MVA front doors - /leads/mva-nyc-split (NYC, Kevin, Leadbloom) and
    /leads/mva-intake (LA-HI). DT-Intake is paused (patch 366) and NLD doesn't
    cover GA/TX/SC, so today LAG is the sole taker of those states (they were
    landing unsold). NOTE: when DT-Intake is un-paused later it is contracted
    for GA/TX too (patch 351), so GA/TX would then be contested between DT and
    LAG at the same rung - a call for Kyler to make at that time.
  * READ-BACK: BUYER_SHEETS gets a 'lag-mva' entry pointing at the
    "Dispositions" tab ($2,200). The scanner matches by phone (restricted to
    leads whose buyer is LAG), so a "Signed / Retained" row queues $2,200 for
    Kyler's approval; everything else just updates the lead's status.
  * KILL SWITCH: set LAG_LIVE=false on Railway to pause delivery instantly.
  * MANUAL PUSH: POST /leads/:id/lag-push (x-api-key) appends one stored lead
    to the sheet and marks it delivered - for a dashboard "send to LAG" button
    and for retrying a failed delivery. Guarded so a lead isn't sent twice.

  krw-backend/server.js (seven edits).

Backup: server.js.pre-367.bak    Safe to re-run: refuses twice.
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
if "patch 367" in src:
    sys.exit("patch 367 already applied - nothing to do")

edits = []

# ── E1: LAG rung on the shared NYC ladder ────────────────────────────────────
edits.append((
"  const NYC_LADDER = [\n",
"  const NYC_LADDER = [\n"
"    { name: 'Legal Advocate Group', priority: 1, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 367: LAG takes GA/TX/SC (states NLD isn't buying), delivered to the LAG Google Sheet\n"))

# ── E2: LAG rung on the Leadbloom-only ladder ────────────────────────────────
edits.append((
"  const LB_ONLY_LADDER = [\n",
"  const LB_ONLY_LADDER = [\n"
"    { name: 'Legal Advocate Group', priority: 1, group: 'lag', cap: null, payout: LAG_PAYOUT, enabled: LAG_LIVE(), states: LAG_STATES },   // patch 367\n"))

# ── E3: LAG sender on /leads/mva-nyc-split ───────────────────────────────────
edits.append((
"      const r = await sendToDtIntake(b, leadState, leadId, PUB);\n"
"      return { result: r.result, accepted: r.accepted };\n"
"    },\n"
"  };",
"      const r = await sendToDtIntake(b, leadState, leadId, PUB);\n"
"      return { result: r.result, accepted: r.accepted };\n"
"    },\n"
"    'Legal Advocate Group': async () => {   // patch 367: deliver to LAG's Google Sheet (Leads tab)\n"
"      if (!LAG_LIVE()) return { result: { status: 'paused', message: 'LAG delivery paused' }, accepted: false };\n"
"      try {\n"
"        const out = await lagAppendRow(b, leadState, leadId);\n"
"        return { result: { status: 'success', message: 'Delivered to Legal Advocate Group sheet', range: out && out.updates && out.updates.updatedRange }, accepted: true };\n"
"      } catch (e) {\n"
"        return { result: { status: 'error', message: 'LAG sheet append failed: ' + e.message }, accepted: false };\n"
"      }\n"
"    },\n"
"  };"))

# ── E4a: LAG rung on the /leads/mva-intake (LA-HI) ladder ─────────────────────
edits.append((
"  const ladder = [{ name: 'DT-Intake', enabled: DT_INTAKE_ON() }]   // patch 359: CH-Intake off, DT catches all\n"
"    .filter(x => x.enabled)",
"  const ladder = [\n"
"      { name: 'Legal Advocate Group', enabled: LAG_LIVE() && LAG_STATES.includes(leadState) },   // patch 367: LAG takes its states on the LA-HI line too\n"
"      { name: 'DT-Intake', enabled: DT_INTAKE_ON() },   // patch 359: CH-Intake off, DT catches all\n"
"    ]\n"
"    .filter(x => x.enabled)"))

# ── E4b: LAG sender on /leads/mva-intake ─────────────────────────────────────
edits.append((
"    'DT-Intake': async () => { const r = await sendToDtIntake(b, leadState, leadId, MVA_INTAKE_PUB); return { result: r.result, accepted: r.accepted }; },   // patch 350\n"
"  };",
"    'DT-Intake': async () => { const r = await sendToDtIntake(b, leadState, leadId, MVA_INTAKE_PUB); return { result: r.result, accepted: r.accepted }; },   // patch 350\n"
"    'Legal Advocate Group': async () => {   // patch 367\n"
"      if (!LAG_LIVE()) return { result: { status: 'paused', message: 'LAG delivery paused' }, accepted: false };\n"
"      try {\n"
"        const out = await lagAppendRow(b, leadState, leadId);\n"
"        return { result: { status: 'success', message: 'Delivered to Legal Advocate Group sheet', range: out && out.updates && out.updates.updatedRange }, accepted: true };\n"
"      } catch (e) {\n"
"        return { result: { status: 'error', message: 'LAG sheet append failed: ' + e.message }, accepted: false };\n"
"      }\n"
"    },\n"
"  };"))

# ── E5: BUYER_SHEETS read-back entry + the LAG write helpers ──────────────────
LAG_HELPERS = r"""

// ── patch 367: Legal Advocate Group (LAG) lead delivery ──────────────────────
// LAG buys the MVA states NLD isn't buying (GA, TX, SC of their requested 7) at
// $2,200/signed. Leads are DELIVERED by appending a row to their Google Sheet's
// "Leads" tab (write path below, isolated from the read-only buyer-sheet scanner
// above), and their dispositions are read back from the "Dispositions" tab by
// the scanner (BUYER_SHEETS 'lag-mva'). Pause delivery with LAG_LIVE=false.
// The sheet is shared with the service account (GOOGLE_SA_KEY) as editor.
const LAG_SHEET_ID  = '1-Kx2As3kn4CZ6iXODQJ8NVwugteEM2U1fJlNjmkxU9I';
const LAG_LEADS_TAB = 'Leads';
const LAG_STATES    = ['GA', 'TX', 'SC'];
const LAG_PAYOUT    = 2200;
function LAG_LIVE() { return (process.env.LAG_LIVE || 'true') !== 'false'; }

// Separate write-scoped service-account token, so the read-only scanner token
// (bsAccessToken, spreadsheets.readonly) is never touched by the write path.
let lagWToken = null, lagWTokenExp = 0;
async function lagWriteToken() {
  if (lagWToken && Date.now() < lagWTokenExp - 60000) return lagWToken;
  const sa = bsSaKey(); if (!sa || !sa.client_email || !sa.private_key) throw new Error('GOOGLE_SA_KEY missing or invalid');
  const now = Math.floor(Date.now() / 1000);
  const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
  const unsigned = b64({ alg: 'RS256', typ: 'JWT' }) + '.' + b64({ iss: sa.client_email, scope: 'https://www.googleapis.com/auth/spreadsheets', aud: 'https://oauth2.googleapis.com/token', iat: now, exp: now + 3600 });
  const sig = require('crypto').createSign('RSA-SHA256').update(unsigned).sign(sa.private_key, 'base64url');
  const body = 'grant_type=' + encodeURIComponent('urn:ietf:params:oauth:grant-type:jwt-bearer') + '&assertion=' + unsigned + '.' + sig;
  const r = await bsHttp('POST', 'https://oauth2.googleapis.com/token', body, { 'Content-Type': 'application/x-www-form-urlencoded' });
  const j = JSON.parse(r.body || '{}');
  if (!j.access_token) throw new Error('LAG token error: ' + (j.error_description || j.error || r.status));
  lagWToken = j.access_token; lagWTokenExp = Date.now() + (j.expires_in || 3600) * 1000;
  return lagWToken;
}

// Append one lead as a row on the LAG "Leads" tab. Column order matches the
// sheet header A:P exactly. Throws on any non-2xx so the caller can treat it as
// a delivery failure (lead shows buyer_rejected, re-pushable via /leads/:id/lag-push).
async function lagAppendRow(b, leadState, leadId) {
  const tok = await lagWriteToken();
  const nowET = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  const row = [[
    nowET,
    b.first_name || '', b.last_name || '',
    String(b.phone || '').replace(/\D/g, ''),
    b.email || '',
    (leadState || '').toUpperCase(),
    b.incident_date || '',
    b.at_fault || '',
    b.injury || b.physical_injury || '',
    b.have_attorney || '',
    '', '', '',                       // Police Report / SOL OK / At-Fault Insurance-UMC: not captured on the lander yet
    b.trustedform_cert_url || '',
    'KRW-' + leadId,
    LAG_PAYOUT,
  ]];
  const url = 'https://sheets.googleapis.com/v4/spreadsheets/' + LAG_SHEET_ID +
    '/values/' + encodeURIComponent("'" + LAG_LEADS_TAB + "'!A:P") +
    ':append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS';
  const r = await bsHttp('POST', url, JSON.stringify({ values: row }), { Authorization: 'Bearer ' + tok, 'Content-Type': 'application/json' });
  if (r.status >= 300) throw new Error('append ' + r.status + ': ' + String(r.body).slice(0, 160));
  return JSON.parse(r.body || '{}');
}
"""
edits.append((
"  { key: 'mva-003',   label: 'MVA-003-LT', buyer: 'MVA-003-LT', vertical: 'MVA', sheet: '10sbja-_waUhHvWnu2t_K_WNHMiKOOE020-70yudDoLI', tab: null, amount: 1700, enabled: true },\n"
"];",
"  { key: 'mva-003',   label: 'MVA-003-LT', buyer: 'MVA-003-LT', vertical: 'MVA', sheet: '10sbja-_waUhHvWnu2t_K_WNHMiKOOE020-70yudDoLI', tab: null, amount: 1700, enabled: true },\n"
"  { key: 'lag-mva',   label: 'Legal Advocate Group', buyer: 'Legal Advocate Group', vertical: 'MVA', sheet: '1-Kx2As3kn4CZ6iXODQJ8NVwugteEM2U1fJlNjmkxU9I', tab: 'Dispositions', amount: 2200, enabled: true },   // patch 367: read LAG dispositions back from the Dispositions tab\n"
"];" + LAG_HELPERS))

# ── E7: manual single-lead push endpoint (dashboard button / retry) ──────────
LAG_PUSH_ROUTE = r"""

// ─── patch 367: push one stored lead to the LAG sheet by id ──────────────────
// Appends a stored lead to LAG's Leads tab and marks it delivered to LAG. For a
// dashboard "send to LAG" action and for retrying a failed delivery. Guarded so
// the same lead isn't delivered twice; ?force=true overrides the state check.
app.post('/leads/:id/lag-push', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [process.env.API_KEY || '64tgzb5ostadx1azjio9crdlduw4vf29', process.env.LEAD_API_KEY || 'krwleads2026secure'];
  if (validKeys.indexOf(key) < 0) return res.status(401).json({ ok: false, error: 'Invalid API key' });
  if (!LAG_LIVE()) return res.status(409).json({ ok: false, error: 'LAG delivery is paused (LAG_LIVE=false)' });
  const id = parseInt(req.params.id, 10);
  if (!id) return res.status(400).json({ ok: false, error: 'Bad lead id' });
  const lr = await pool.query('SELECT id, state, raw FROM leads WHERE id=$1', [id]);
  if (!lr.rows.length) return res.status(404).json({ ok: false, error: 'Lead not found' });
  const lead = lr.rows[0]; const b = lead.raw || {}; const st = (lead.state || '').toUpperCase().trim();
  if (b.lag_delivered) return res.json({ ok: true, already: true, krw_id: id, message: 'Already delivered to LAG' });
  if (req.query.force !== 'true' && LAG_STATES.indexOf(st) < 0) {
    return res.status(400).json({ ok: false, error: 'State ' + st + ' is not a LAG state (' + LAG_STATES.join('/') + '); pass ?force=true to override' });
  }
  try {
    await lagAppendRow(b, st, id);
    await pool.query(
      "UPDATE leads SET status='forwarded', buyer_status='Accepted', billable=false, revenue=0, " +
      "raw = COALESCE(raw,'{}'::jsonb) || $1::jsonb WHERE id=$2",
      [JSON.stringify({ buyer_name: 'Legal Advocate Group', lag_delivered: true, lag_pushed_at: new Date().toISOString() }), id]);
    return res.json({ ok: true, krw_id: id, buyer: 'Legal Advocate Group' });
  } catch (e) {
    return res.status(502).json({ ok: false, error: e.message, krw_id: id });
  }
});"""
edits.append((
"  return res.json({ ok: accepted, result: accepted ? 'success' : 'rejected', message: accepted ? 'Lead accepted' : (result.message || 'Lead rejected'),\n"
"    buyer: buyerName, krw_id: leadId });\n"
"});",
"  return res.json({ ok: accepted, result: accepted ? 'success' : 'rejected', message: accepted ? 'Lead accepted' : (result.message || 'Lead rejected'),\n"
"    buyer: buyerName, krw_id: leadId });\n"
"});" + LAG_PUSH_ROUTE))

# ── verify every anchor is unique, then apply ────────────────────────────────
for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit E%d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-367.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 367 applied to", FILE)
