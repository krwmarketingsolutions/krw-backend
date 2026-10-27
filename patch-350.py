#!/usr/bin/env python3
"""
patch-350.py  --  DT-Intake: new MVA intake buyer, replaces CH-Intake and
                  LT-Intake on every live path (Kyler, Oct 26)

WHAT CHANGES
  - New buyer "DT-Intake". Leads post as JSON to the GoHighLevel inbound
    webhook they supplied (DT_INTAKE_WEBHOOK on Railway overrides the URL in
    code). Payload matches their sample: firstName, lastName, phone (+1 E.164),
    email, city, state, dateOfAccident (YYYY-MM-DD), accidentType, fault,
    policeReport, injured, injuries, medicalTreatment, hospitalizedOrSurgery,
    otherDriverInsured, clientInsured, alreadyHiredAttorney, caseFacts,
    leadSource, campaign - plus krwId and the TrustedForm cert so they can
    reference our ID back on the sheet.
  - Routing, every path (live ladders, LA-HI intake line, Leadbloom2 line,
    janitor retries, resend-ladder):
        DT-Intake first on the 13 intake states + Arizona  ("AZ over NLD")
        NLD CPA on its other states (10/day cap, pause flag unchanged)
        DT-Intake catches anything left, any state
    Leadbloom keeps NLD first on OK and NE (patch 326); everyone else sends
    OK/NE to intake first, as before. CH-Intake and LT-Intake are never
    offered a lead on any path. 003 stays off.
  - Disposition sheet: their "Lead Bloom - Dispositions" sheet is added to the
    scanner. Statuses: New Lead / Contacted -> in outreach; Signed -> billable
    ($2,500) into the approval queue; Unresponsive / Not Qualified / Duplicate
    -> rejected. Matched by phone. CH and LT sheets keep scanning for the
    leads they already hold.
  - Funnel: DT-Intake node added; publisher edges now DT-Intake + NLD.
  - /leads/:id/attach-cert-and-send accepts buyer "DT-Intake".
  - New test route: POST /leads/forward-to-dt-intake { krw_id } re-sends an
    existing lead; { test: true, phone } posts a fake "Test" lead (vendor code
    KRW-TEST) so you can see it land in their CRM before real traffic.
  - LA-HI's MVA line (/leads/mva-intake) now also accepts AZ.
  - Kill switch: DT_INTAKE_ENABLED=false on Railway takes DT out of every
    ladder (leads then go NLD-or-held, since CH/LT are gone).

Backup: server.js.pre-350.bak    Safe to re-run: refuses twice.
Deploy:
  python3 ~/Downloads/patch-350.py
  cd ~/krw/krw-backend && node --check server.js && git add server.js && git commit -m "patch 350: DT-Intake replaces CH/LT intake" && git push
"""
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd()), os.path.expanduser("~/krw")):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None


ROOT = find_root() or sys.exit("STOPPED: could not find krw-backend - run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()

if "patch 350" in src:
    sys.exit("patch 350 already applied - nothing to do")
if "patch 333" not in src:
    sys.exit("STOPPED: patch 333 is not applied - this patch expects the Oct 19 server")

edits = []  # (label, old, new)


def rep(label, old, new, count=1):
    edits.append((label, old, new, count))


# ──────────────────────────────────────────────────────────────────────────────
# 1. Ladders in /leads/mva-funnel
# ──────────────────────────────────────────────────────────────────────────────
LADDER_START = "  // LT-Intake posts to a VICIdial dialer over GET (spec from Adam, Sep 16). It goes live"
LADDER_END = "  const LADDER_FOR_PUB = PUB === 'KRW-LEADBLOOM-MVA' ? LB_ONLY_LADDER : LEADBLOOM_LADDER;"
NEW_LADDERS = r"""  // patch 350 (Kyler, Oct 26): DT-Intake replaces CH-Intake and LT-Intake as
  // the intake buyer for every MVA publisher. It takes the 13 intake states
  // plus Arizona ahead of NLD ("give them AZ over NLD"); NLD keeps its other
  // states; DT catches anything nobody else covers. Leadbloom keeps NLD first
  // on OK and NE (patch 326) - everyone else sends those to intake first, as
  // before. CH and LT are never offered a lead on any path; their disposition
  // sheets keep syncing for leads they already hold. 003 stays off.
  const DT_ON        = DT_INTAKE_ON();
  const DT_STATES    = INTAKE_STATES.concat(['AZ']);
  const DT_LB_STATES = INTAKE_STATES.filter(s => s !== 'OK' && s !== 'NE').concat(['AZ']);
  const NYC_LADDER = [
    { name: 'DT-Intake',  priority: 1, group: 'intake',          cap: null, payout: 2500, enabled: DT_ON,       states: DT_STATES },
    { name: 'NLD CPA',    priority: 2, group: 'nld',             cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318
    { name: 'DT-Intake',  priority: 3, group: 'intake-overflow', cap: null, payout: 2500, enabled: DT_ON,       states: 'ALL' },
    { name: 'MVA-003-LT', priority: 4, group: '003',             cap: 5,    payout: 1700, enabled: false,       states: 'ALL' },   // patch 305: 003 off
  ];
  const LEADBLOOM_LADDER = NYC_LADDER;   // the shared ladder: NYC, Kevin, everyone but Leadbloom
  const LB_ONLY_LADDER = [
    { name: 'DT-Intake',  priority: 1, group: 'intake',          cap: null, payout: 2500, enabled: DT_ON,       states: DT_LB_STATES },
    { name: 'NLD CPA',    priority: 2, group: 'nld',             cap: 10,   payout: 2250, enabled: !NLD_PAUSED, states: NLD_ONLY_STATES },   // patch 318
    { name: 'DT-Intake',  priority: 3, group: 'intake-overflow', cap: null, payout: 2500, enabled: DT_ON,       states: 'ALL' },
  ];
  const LADDER_FOR_PUB = PUB === 'KRW-LEADBLOOM-MVA' ? LB_ONLY_LADDER : LEADBLOOM_LADDER;"""

# 2. DT sender in the mva-funnel senders map
OLD_SENDERS_TAIL = """    'LT-Intake': async () => {
      const r = await sendToLtIntake(b, leadState, leadId);
      return { result: r.result, accepted: r.accepted };
    },
  };

  // ── Walk the ladder"""
NEW_SENDERS_TAIL = """    'LT-Intake': async () => {
      const r = await sendToLtIntake(b, leadState, leadId);
      return { result: r.result, accepted: r.accepted };
    },
    'DT-Intake': async () => {   // patch 350
      const r = await sendToDtIntake(b, leadState, leadId, PUB);
      return { result: r.result, accepted: r.accepted };
    },
  };

  // ── Walk the ladder"""
rep("mva-funnel senders", OLD_SENDERS_TAIL, NEW_SENDERS_TAIL)

# 3. DT helpers + test route, placed next to the LT helpers
LT_MANUAL_ANCHOR = "// Manual / test: POST { krw_id } re-sends an existing lead to LT-Intake and returns what"
DT_HELPERS = r"""// ─── DT-Intake (patch 350, Oct 26) ───────────────────────────────────────────
// GoHighLevel inbound webhook. Payload mirrors the sample they sent; extra
// keys (krwId, trustedFormCertUrl, consentTimestamp) ride along so their CRM
// can carry our reference. No publisher ID ever goes out - leadSource is the
// brand only. Env on Railway:
//   DT_INTAKE_ENABLED  default true  - "false" removes DT from every ladder
//   DT_INTAKE_WEBHOOK  default the URL below
const DT_INTAKE_WEBHOOK_DEFAULT = 'https://services.leadconnectorhq.com/hooks/e35fEKrl9gkdsZYGbQJZ/webhook-trigger/092e51f3-2598-43b8-b04b-712d1ae1f8c2';
function DT_INTAKE_ON() { return (process.env.DT_INTAKE_ENABLED || 'true') !== 'false'; }
function dtIsoDate(v) {
  const s = String(v == null ? '' : v).trim(); if (!s) return undefined;
  let m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/); if (m) return m[1] + '-' + m[2].padStart(2, '0') + '-' + m[3].padStart(2, '0');
  m = s.match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2,4})/);
  if (m) { const y = m[3].length === 2 ? '20' + m[3] : m[3]; return y + '-' + m[1].padStart(2, '0') + '-' + m[2].padStart(2, '0'); }
  const t = Date.parse(s); return isNaN(t) ? s : new Date(t).toISOString().slice(0, 10);
}
function dtYesNo(v, dflt) { const s = String(v == null ? '' : v).trim().toLowerCase(); if (!s) return dflt; if (/^(y|yes|true|1)$/.test(s)) return 'Yes'; if (/^(n|no|false|0)$/.test(s)) return 'No'; return v; }
function dtIntakePayload(b, leadState, leadId, pub) {
  const digits = String(b.phone || '').replace(/\D/g, '').replace(/^1(?=\d{10}$)/, '');
  const atFault = String(b.at_fault || '').trim().toLowerCase();
  const fault = /^(no|n|false|0)$/.test(atFault) ? 'Another Driver' : /^(yes|y|true|1)$/.test(atFault) ? 'I Was at Fault' : /partial/.test(atFault) ? 'I Was Partially at Fault' : 'Not Sure';
  const summary = String(b.summary || '');
  const sumType = (summary.split(/\s*[·|]\s*/)[0] || '').trim();
  const accidentType = b.accident_type || (/\b(truck|semi|commercial|18[- ]wheeler)\b/i.test(summary + ' ' + (b.injury || '')) ? 'Truck / Commercial Vehicle Accident'
    : /\b(uber|lyft|rideshare)\b/i.test(summary) ? 'Rideshare (Uber / Lyft)' : /\bmotorcycle\b/i.test(summary) ? 'Motorcycle Accident'
    : /\bpedestrian\b/i.test(summary) ? 'Pedestrian Struck by Vehicle' : /\b(bicycle|bike)\b/i.test(summary) ? 'Bicycle Accident'
    : /hit[- ]and[- ]run/i.test(summary) ? 'Hit-and-Run' : (sumType && /accident|collision/i.test(sumType) ? sumType : 'Car Accident'));
  const treated = dtYesNo(b.doctor_treatment, undefined);
  const caseFacts = [b.case_description || b.description || b.case_facts || null, summary || null, b.injury ? 'Injury: ' + b.injury : null].filter(Boolean).join(' | ').slice(0, 1500) || undefined;
  const p = {
    firstName: b.first_name, lastName: b.last_name,
    phone: digits.length === 10 ? '+1' + digits : String(b.phone || ''),
    email: b.email, city: b.city || undefined, state: leadState,
    dateOfAccident: dtIsoDate(b.incident_date),
    accidentType, role: b.role || undefined, fault,
    policeReport: dtYesNo(b.police_report, 'Not Sure'),
    injured: dtYesNo(b.physical_injury, b.injury ? 'Yes' : 'Not Sure'),
    injuries: b.injury || undefined,
    medicalTreatment: b.medical_treatment || (treated === 'Yes' ? 'Yes' : treated === 'No' ? 'Not Yet' : undefined),
    hospitalizedOrSurgery: dtYesNo(b.hospitalized || b.surgery, undefined),
    otherDriverInsured: dtYesNo(b.other_driver_insured || b.defendant_insured, 'Unknown'),
    clientInsured: dtYesNo(b.client_insured || b.has_insurance, undefined),
    alreadyHiredAttorney: dtYesNo(b.have_attorney, 'No'),
    caseFacts,
    leadSource: pub === 'KRW-LEADBLOOM-MVA' ? 'LeadBloom' : 'KRW',
    campaign: 'MVA Web Intake',
    krwId: leadId != null ? 'KRW-' + leadId : undefined,
    trustedFormCertUrl: b.trustedform_cert_url || undefined,
    consentTimestamp: new Date().toISOString(),
  };
  Object.keys(p).forEach(k => { if (p[k] === undefined || p[k] === null || p[k] === '') delete p[k]; });
  return p;
}
async function sendToDtIntake(b, leadState, leadId, pub) {
  if (!DT_INTAKE_ON()) throw new Error('DT-Intake is disabled (DT_INTAKE_ENABLED=false)');
  const url = process.env.DT_INTAKE_WEBHOOK || DT_INTAKE_WEBHOOK_DEFAULT;
  const p = dtIntakePayload(b, leadState, leadId, pub);
  const r = await postJSON(url, p);
  let out; try { out = JSON.parse(r.body); } catch (e) { out = { status: r.status, raw: String(r.body || '').slice(0, 300) }; }
  const accepted = r.status >= 200 && r.status < 300 && !(out && String(out.status || '').toLowerCase() === 'error');
  console.log(`[DT-Intake] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} | http ${r.status} ${String(r.body || '').slice(0, 100)}`);
  return { accepted, result: { status: accepted ? 'success' : 'error', http: r.status, message: (out && (out.message || out.msg)) || (accepted ? 'accepted' : String(r.body || '').slice(0, 200)), raw: out }, sent: p };
}

// Manual / test: POST { krw_id } re-sends an existing lead to DT-Intake.
// { test: true, phone } posts a fake "Test" lead so they can see one land.
app.post('/leads/forward-to-dt-intake', async (req, res) => {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  const validKeys = [process.env.API_KEY || '64tgzb5ostadx1azjio9crdlduw4vf29', process.env.LEAD_API_KEY || 'krwleads2026secure'];
  const bearerOk = typeof callerRole === 'function' && callerRole(req) === 'admin';
  if (!validKeys.includes(key) && !bearerOk) return res.status(401).json({ ok: false, error: 'Unauthorized' });
  const { krw_id, test, phone } = req.body || {};
  try {
    if (test === true) {
      const b = { first_name: 'Test', last_name: 'KRW-TEST', phone: phone || '5550001234', email: 'test@krwmarketingsolutions.com', city: 'Austin',
        incident_date: new Date().toISOString().slice(0, 10), at_fault: 'No', have_attorney: 'No', physical_injury: 'Yes', doctor_treatment: 'Yes',
        injury: 'TEST LEAD - please disregard', summary: 'TEST LEAD · sent by KRW to confirm delivery · not a real claimant' };
      const r = await sendToDtIntake(b, 'TX', null, 'KRW-TEST');
      return res.json({ ok: r.accepted, test: true, sent: r.sent, response: r.result });
    }
    if (!krw_id) return res.status(400).json({ ok: false, error: 'krw_id required (or { test: true, phone })' });
    const q = await pool.query('SELECT * FROM leads WHERE id=$1', [krw_id]);
    if (!q.rows[0]) return res.status(404).json({ ok: false, error: 'Lead not found' });
    const row = q.rows[0];
    if (row.billable === true) return res.status(409).json({ ok: false, error: 'Lead is billable - refusing to touch it' });
    const b = Object.assign({}, row.raw || {}); b.phone = b.phone || row.phone; b.first_name = b.first_name || row.first_name; b.last_name = b.last_name || row.last_name; b.email = b.email || row.email;
    const st = (row.state || b.state || '').toUpperCase();
    const r = await sendToDtIntake(b, st, row.id, row.publisher_sub);
    if (r.accepted) {
      await pool.query(`UPDATE leads SET status='forwarded', buyer_status='Accepted', buyer_error=NULL,
          buyer_response = COALESCE(buyer_response,'{}'::jsonb) || $1::jsonb,
          raw = COALESCE(raw,'{}'::jsonb) || $2::jsonb WHERE id=$3`,
        [JSON.stringify({ manual_forward: { buyer: 'DT-Intake', response: r.result, at: new Date().toISOString() } }),
         JSON.stringify({ buyer_name: 'DT-Intake', manual_forward: true, manual_forward_at: new Date().toISOString() }), row.id]);
    }
    return res.json({ ok: r.accepted, krw_id: row.id, sent: r.sent, response: r.result });
  } catch (e) {
    return res.status(502).json({ ok: false, error: e.message });
  }
});

"""
rep("DT helpers", LT_MANUAL_ANCHOR, DT_HELPERS + LT_MANUAL_ANCHOR)

# 4. LA-HI intake line
rep("LA-HI states",
    "const MVA_INTAKE_STATES = ['FL','GA','WI','TX','MI','IN','IL','MN','CO','MO','NE','OK','TN'];",
    "const MVA_INTAKE_STATES = ['FL','GA','WI','TX','MI','IN','IL','MN','CO','MO','NE','OK','TN','AZ'];   // patch 350: DT buys AZ")
rep("LA-HI ladder",
    "  const ladder = [{ name: 'CH-Intake', enabled: true }, { name: 'LT-Intake', enabled: Boolean(process.env.LT_INTAKE_PASS) }]",
    "  const ladder = [{ name: 'DT-Intake', enabled: DT_INTAKE_ON() }]   // patch 350: DT replaces CH/LT on this line")
rep("LA-HI senders",
    "    'LT-Intake': async () => { const r = await sendToLtIntake(b, leadState, leadId); return { result: r.result, accepted: r.accepted }; },\n  };",
    "    'LT-Intake': async () => { const r = await sendToLtIntake(b, leadState, leadId); return { result: r.result, accepted: r.accepted }; },\n    'DT-Intake': async () => { const r = await sendToDtIntake(b, leadState, leadId, MVA_INTAKE_PUB); return { result: r.result, accepted: r.accepted }; },   // patch 350\n  };")

# 5. Leadbloom2 line (dormant, but it was LT-only - now DT-only)
rep("LB2 enabled check",
    "  if (Boolean(process.env.LT_INTAKE_PASS) === false) {\n    const why = 'Buyer not configured (LT_INTAKE_PASS)';",
    "  if (!DT_INTAKE_ON()) {   // patch 350: this line now goes to DT-Intake\n    const why = 'Buyer not configured (DT_INTAKE_ENABLED=false)';")
rep("LB2 send",
    "    const r = await sendToLtIntake(b, leadState, leadId);\n    accepted = r.accepted; result = r.result;\n  } catch (err) { result = { status: 'error', message: err.message }; }",
    "    const r = await sendToDtIntake(b, leadState, leadId, 'KRW-LEADBLOOM2-MVA');   // patch 350\n    accepted = r.accepted; result = r.result;\n  } catch (err) { result = { status: 'error', message: err.message }; }")
rep("LB2 stamp",
    "     JSON.stringify({ buyer_name: 'LT-Intake', routing_attempts: ['LT-Intake:' + (accepted ? 'accepted' : 'rejected')] }), leadId]);\n  console.log(`[Leadbloom2] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} | -> LT-Intake`);\n  return res.json({ ok: accepted, result: accepted ? 'success' : 'rejected', message: accepted ? 'Lead accepted' : (result.message || 'Lead rejected'), buyer: 'LT-Intake', krw_id: leadId });",
    "     JSON.stringify({ buyer_name: 'DT-Intake', routing_attempts: ['DT-Intake:' + (accepted ? 'accepted' : 'rejected')] }), leadId]);\n  console.log(`[Leadbloom2] ${accepted ? '✓' : '✕'} ${b.first_name} ${b.last_name} | ${leadState} | -> DT-Intake`);\n  return res.json({ ok: accepted, result: accepted ? 'success' : 'rejected', message: accepted ? 'Lead accepted' : (result.message || 'Lead rejected'), buyer: 'DT-Intake', krw_id: leadId });")

# 6. janSend: DT branch
rep("janSend DT",
    "  return { result: { error: 'unknown buyer' }, accepted: false };",
    "  if (buyer === 'DT-Intake') {   // patch 350\n    const r = await sendToDtIntake(b, st, leadId, pub);\n    return { result: r.result, accepted: r.accepted };\n  }\n  return { result: { error: 'unknown buyer' }, accepted: false };")

# 7. janitor rungs
JAN_START = "      const rungs = [];\n      const janPushIntake = () => {"
JAN_END = "      if (!rungs.length) { lines.push(`lead ${row.id}"
JAN_NEW = """      // patch 350: DT-Intake replaces CH/LT on every retry path. Intake
      // states + AZ go to DT first (Leadbloom: NLD keeps OK/NE first, patch
      // 326); NLD takes its other states; DT catches the rest.
      const rungs = [];
      const dtOn = DT_INTAKE_ON();
      const wantNld = !NLD_PAUSED && JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX;   // patch 318
      const dtFirst = rowPub === 'KRW-LEADBLOOM-MVA'
        ? (st === 'AZ' || (JAN_INTAKE_STATES.includes(st) && st !== 'OK' && st !== 'NE'))
        : (st === 'AZ' || JAN_INTAKE_STATES.includes(st));
      if (dtOn && dtFirst) rungs.push('DT-Intake');
      if (wantNld) rungs.push('NLD CPA');
      if (dtOn && !rungs.includes('DT-Intake')) rungs.push('DT-Intake');
"""

# 8. resend-ladder rungs
RS_START = "    const rungs = [];\n    const pushIntake = () => {"
RS_END = "    let plan = rungs.filter((x, i) => rungs.indexOf(x) === i);"
RS_NEW = """    // patch 350: DT-Intake replaces CH/LT. Intake states + AZ go to DT first
    // (Leadbloom: NLD keeps OK/NE first); NLD on its other states; DT catches the rest.
    const rungs = [];
    const dtOn = DT_INTAKE_ON();
    const wantNld = !NLD_PAUSED && NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10;   // patch 318
    const dtFirst = isLeadbloom
      ? (st === 'AZ' || (JAN_INTAKE_STATES.includes(st) && st !== 'OK' && st !== 'NE'))
      : (st === 'AZ' || JAN_INTAKE_STATES.includes(st));
    if (dtOn && dtFirst) rungs.push('DT-Intake');
    if (wantNld) rungs.push('NLD CPA');
    if (dtOn) rungs.push('DT-Intake');
"""

# 9. attach-cert-and-send
rep("attach-cert ALLOWED",
    "    const ALLOWED = ['NLD CPA', 'LT-Intake', 'CH-Intake', 'MVA-003-LT'];",
    "    const ALLOWED = ['NLD CPA', 'DT-Intake', 'LT-Intake', 'CH-Intake', 'MVA-003-LT'];   // patch 350")

# 10. funnel
rep("funnel known buyers",
    "    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'LT-Intake'];",
    "    const mvaKnownBuyers = ['NLD CPA', 'MVA-003-LT', 'CH-Intake', 'LT-Intake', 'DT-Intake'];   // patch 350")
rep("funnel routing edges",
    "        'KRW-KANTHONY-RS':  ['CH-Intake', 'LT-Intake', 'NLD CPA'],\n        [LEADBLOOM_PUB_ID]: ['CH-Intake', 'LT-Intake', 'NLD CPA'],\n        'KRW-NYC-MVA':      ['CH-Intake', 'LT-Intake', 'NLD CPA'],",
    "        // patch 350: DT-Intake replaces CH/LT as the intake buyer for everyone\n        'KRW-KANTHONY-RS':  ['DT-Intake', 'NLD CPA'],\n        [LEADBLOOM_PUB_ID]: ['DT-Intake', 'NLD CPA'],\n        'KRW-NYC-MVA':      ['DT-Intake', 'NLD CPA'],")

# 11. buyer sheet scanner
CH_SHEET_LINE = "  { key: 'ch-intake', label: 'CH-Intake',  buyer: 'CH-Intake',  vertical: 'MVA', sheet: '1vlM4f8lqOHemrRZ1IhYdS9amU826GNV5GJS8nfGRgE0', tab: null, campaigns: ['roblox-mt','roblox-chad','rideshare-chad'], amount: 2250, enabled: true },"
rep("buyer sheets", CH_SHEET_LINE,
    CH_SHEET_LINE + "\n  // patch 350: DT-Intake's auto-updated pipeline sheet (lead_id / status / name / phone / received_date / retainer_date / reason / updated_at).\n  // Statuses: New Lead, Contacted, Signed (billable), Unresponsive, Not Qualified, Duplicate. Matched by phone.\n  { key: 'dt-intake', label: 'DT-Intake',  buyer: 'DT-Intake',  vertical: 'MVA', sheet: '1_pvfSFxsvK0saUvwQFmG0YHG7f4ySs0TysywLqhzRBQ', tab: null, amount: 2500, enabled: true },")
rep("scanner New Lead status",
    "|\\bcalled\\b|^new$/i;",
    "|\\bcalled\\b|^new$|^new lead$/i;   // patch 350: DT's first stage")

# 12. publisher-note scrub
rep("note scrub",
    "  note = note.replace(/\\b(NLD(\\s+(CPA|Rideshare))?|CH-Intake|LT-Intake|MVA-003(-LT)?|LAR-MVA-CPA)\\b(\\s*sheet)?/gi, '');",
    "  note = note.replace(/\\b(NLD(\\s+(CPA|Rideshare))?|CH-Intake|LT-Intake|DT-Intake|MVA-003(-LT)?|LAR-MVA-CPA)\\b(\\s*sheet)?/gi, '');   // patch 350")
rep("note scrub parens",
    "  note = note.replace(/\\s*\\([^)]*(NLD|CH[-\\s]?Intake|LT[-\\s]?Intake|MVA[-\\s]?003|LAR)[^)]*\\)/gi, '');",
    "  note = note.replace(/\\s*\\([^)]*(NLD|CH[-\\s]?Intake|LT[-\\s]?Intake|DT[-\\s]?Intake|MVA[-\\s]?003|LAR)[^)]*\\)/gi, '');")

# ──────────────────────────────────────────────────────────────────────────────
# verify every anchor BEFORE touching anything
# ──────────────────────────────────────────────────────────────────────────────
problems = []
for label, old, new, count in edits:
    n = src.count(old)
    if n != count:
        problems.append("%s: anchor found %d time(s), expected %d" % (label, n, count))
for label, a, b in (("ladders", LADDER_START, LADDER_END), ("janitor rungs", JAN_START, JAN_END), ("resend rungs", RS_START, RS_END)):
    for nm, s in (("start", a), ("end", b)):
        if src.count(s) != 1:
            problems.append("%s %s anchor found %d time(s), expected 1" % (label, nm, src.count(s)))
    if src.count(a) == 1 and src.count(b) == 1 and src.index(b) <= src.index(a):
        problems.append("%s: markers out of order" % label)
if problems:
    print("STOPPED - nothing changed. Anchors did not match:")
    for p in problems:
        print("  - " + p)
    sys.exit(1)

bak = FILE + ".pre-350.bak"
shutil.copyfile(FILE, bak)

# ranged replacements
a, b = src.index(LADDER_START), src.index(LADDER_END) + len(LADDER_END)
src = src[:a] + NEW_LADDERS + src[b:]
a, b = src.index(JAN_START), src.index(JAN_END)
src = src[:a] + JAN_NEW + src[b:]
a, b = src.index(RS_START), src.index(RS_END)
src = src[:a] + RS_NEW + src[b:]
# literal replacements
for label, old, new, count in edits:
    src = src.replace(old, new, count)

open(FILE, "w", encoding="utf-8").write(src)
print("Patched OK: %s" % FILE)
print("Backup:     %s" % bak)
print("Next: cd ~/krw/krw-backend && node --check server.js && git add server.js && git commit -m \"patch 350: DT-Intake replaces CH/LT intake\" && git push")
print("Then: curl -s -X POST https://krw-backend-production.up.railway.app/leads/forward-to-dt-intake -H 'x-api-key: krwleads2026secure' -H 'Content-Type: application/json' -d '{\"test\":true,\"phone\":\"5550001234\"}'")
