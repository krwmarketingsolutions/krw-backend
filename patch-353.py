#!/usr/bin/env python3
"""
patch-353.py  --  onboarding tracker store (backend half; dashboard half is
                  portal patch-353)

Buyers and partners being onboarded get their own table so the Outreach tab
can show them on every device, not from one browser's storage.

  table onboarding_clients: id, name, code, stage, summary, email, notes,
                            sort, archived, created_at, updated_at
  GET    /onboarding            list (active; ?all=1 includes archived)
  POST   /onboarding            create  {name, code?, stage?, summary?, email?, notes?}
  PUT    /onboarding/:id        update any of those fields
  POST   /onboarding/:id/note   append a dated line to notes  {note}
  DELETE /onboarding/:id        archive (never deletes)
All admin-key / login-token routes. Stages: prospect, pre-contract,
contract-sent, signed, setup, testing, live, scaling.

First GET on an empty table seeds the five clients from the Oct 26 tracker.

Backup: server.js.pre-353.bak    Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd()), os.path.expanduser("~/krw")):
        if os.path.isdir(os.path.join(c, "krw-backend")): return c
ROOT = find_root() or sys.exit("STOPPED: could not find krw-backend - run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()
if "patch 353" in src: sys.exit("patch 353 already applied - nothing to do")
ANCHOR = "// ─── end patch 275 ────────────────────────────────────────────────────────────"
if src.count(ANCHOR) != 1: sys.exit("STOPPED - end-of-patch-275 anchor found %d time(s), expected 1. Nothing changed." % src.count(ANCHOR))

BLOCK = r'''
// ─── patch 353: onboarding tracker (buyers / partners being brought on) ──────
let obTableReady = false;
async function obEnsureTable() {
  if (obTableReady) return;
  await pool.query(`CREATE TABLE IF NOT EXISTS onboarding_clients (
    id SERIAL PRIMARY KEY, name TEXT NOT NULL, code TEXT, stage TEXT NOT NULL DEFAULT 'prospect',
    summary TEXT, email TEXT, notes TEXT, sort INT DEFAULT 0, archived BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
  obTableReady = true;
}
const OB_STAGES = ['prospect', 'pre-contract', 'contract-sent', 'signed', 'setup', 'testing', 'live', 'scaling'];
const OB_SEED = [
  { name: 'Daniel Intake', code: 'DT', stage: 'live', email: 'danielrochelegal@gmail.com', sort: 1,
    summary: 'MVA buyer, live as DT-Intake. GA, TX, MO, FL, IL, IN, MN. $2,500 per signed case. IO drafted, not sent.',
    notes: [
      'Contact: Daniel Roche, danielrochelegal@gmail.com. Firm legal name and address still needed for the IO.',
      'Vertical: MVA, exclusive web form leads from Leadbloom, paid per signed retainer.',
      'Price per CPA: $2,500 standard (he offered $2,400 to $2,500). Rate increase in the IO: $2,700 to $3,200 once volume is consistent.',
      'States: GA, TX, MO, FL, IL, IN, MN (patch 352, Oct 26). AZ went back to NLD. CH-Intake is the catch-all for everything else.',
      'Intake: 7 agents, 7 AM to 10 PM PST, 5 minute first contact target. Stack: Level Up Marketing, GoHighLevel.',
      'Volume: told them 10 to 15 leads/day, expecting 5 to 8/day at launch.',
      'Referral: Tom gets $100 per CPA (flat). Net on standard tier: $2,500 - $1,700 Leadbloom - $100 Tom = $700.',
      'Criteria: his webform criteria received Oct 23. Leadbloom\'s Oct 5 criteria are already tighter, no loosening needed.',
      'IO (KRW-IO-2026-10): commercial $4,000 tier removed, full replacement guarantee for leads invoiced or paid inside the 14 day window.',
      'Oct 26: patch-350 deployed, test lead accepted by his GHL webhook (id gja1gkpgUDMLyjDDveiG), disposition sheet scanning (tab "Dispositions").',
      'Open: confirm the test lead showed in his CRM, firm name/address, send IO, share the sheet to waltersonkyle@gmail.com, Aiden call (AEO), Lou setup (CTV).'
    ].join('\n') },
  { name: 'Push IQ', code: 'PUSHIQ', stage: 'pre-contract', email: '', sort: 2,
    summary: 'Jack. Large buyer, 18 states, wants signed retainers. Budget due Wednesday Oct 28.',
    notes: [
      'Contact: Jack.',
      'Product: likely signed retainers, not raw leads. Needs an intake team signing on our behalf and transferring to their attorneys (CH re engage, or Donnie and Doug if Thursday goes well).',
      'States: 18 state list coming from Jack.',
      'Sent: MVA overview one-pager (Oct 26).',
      'Open: Jack\'s state list and budget (Wed Oct 28), intake team for signing, pricing per state once budget is in, IO.'
    ].join('\n') },
  { name: 'Donnie and Doug', code: 'DD', stage: 'prospect', email: '', sort: 3,
    summary: 'Full intake service. Can generate, process, sign and transfer. Retreaver. Pricing Thursday Oct 29.',
    notes: [
      'What they are: full intake service. They can generate and process leads themselves, run them through a retainer process in their call center, verify them, and transfer signed clients to the law firm.',
      'Tracking: Retreaver (KRW already has Retreaver posting integrations).',
      'Two possible roles: (1) intake team for KRW, the piece Push IQ needs for signed retainers; (2) publisher feeding Push IQ (Jack) if Thursday goes well.',
      'Terms discussed, no contract sent, no pricing set. Follow up meeting Thursday Oct 29.',
      'Open: pricing, contract (publisher MSA + IO if they publish, intake services agreement if they sign for us), Retreaver campaign setup, decide which role first.'
    ].join('\n') },
  { name: 'B1 Marketing', code: 'B1', stage: 'pre-contract', email: '', sort: 4,
    summary: 'Freddie. Large buyer, about 18 states. One-pager sent, waiting on their intake and startup team to set a pricing meeting.',
    notes: [
      'Contact: Freddie (Freddy Wahler, B1 Marketing Group / Intake Firewall, connected on LinkedIn Oct 8).',
      'Large buyer, roughly 18 states, similar footprint to Push IQ.',
      'Sent: MVA one-page report (Oct 26).',
      'Open: wait for their team\'s outreach, pricing meeting, state list, intake details, IO.'
    ].join('\n') },
  { name: 'Sunshine', code: 'SUN', stage: 'prospect', email: '', sort: 5,
    summary: 'JJ intro. First meeting Tuesday Oct 27, 1:30 PM Central (11:30 AM Pacific).',
    notes: [
      'Source: JJ introduction.',
      'Known so far: nothing yet. Role (buyer, publisher, intake), states, pricing and fit all to be learned in the meeting.',
      'Open: meeting, then decide if and how they fit.'
    ].join('\n') },
];
function obClean(b) {
  const o = {};
  for (const k of ['name', 'code', 'stage', 'summary', 'email', 'notes']) if (b[k] !== undefined) o[k] = b[k] == null ? null : String(b[k]).trim();
  if (b.sort !== undefined) o.sort = parseInt(b.sort, 10) || 0;
  if (b.archived !== undefined) o.archived = b.archived === true || b.archived === 'true';
  if (o.stage && !OB_STAGES.includes(o.stage)) throw new Error('stage must be one of ' + OB_STAGES.join(', '));
  return o;
}
app.get('/onboarding', requireKey, async (req, res) => {
  try {
    await obEnsureTable();
    const n = (await pool.query('SELECT COUNT(*)::int AS n FROM onboarding_clients')).rows[0].n;
    if (n === 0) for (const s of OB_SEED) await pool.query('INSERT INTO onboarding_clients (name, code, stage, summary, email, notes, sort) VALUES ($1,$2,$3,$4,$5,$6,$7)', [s.name, s.code, s.stage, s.summary, s.email, s.notes, s.sort]);
    const all = req.query.all === '1';
    const r = await pool.query('SELECT * FROM onboarding_clients' + (all ? '' : ' WHERE archived IS NOT TRUE') + ' ORDER BY sort, id');
    res.json({ ok: true, stages: OB_STAGES, clients: r.rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/onboarding', requireKey, async (req, res) => {
  try {
    await obEnsureTable();
    const o = obClean(req.body || {});
    if (!o.name) return res.status(400).json({ ok: false, error: 'name is required' });
    const r = await pool.query('INSERT INTO onboarding_clients (name, code, stage, summary, email, notes, sort) VALUES ($1,$2,$3,$4,$5,$6,COALESCE($7,(SELECT COALESCE(MAX(sort),0)+1 FROM onboarding_clients))) RETURNING *',
      [o.name, o.code || null, o.stage || 'prospect', o.summary || null, o.email || null, o.notes || null, o.sort == null ? null : o.sort]);
    res.json({ ok: true, client: r.rows[0] });
  } catch (e) { res.status(e.message.startsWith('stage must') ? 400 : 500).json({ ok: false, error: e.message }); }
});
app.put('/onboarding/:id', requireKey, async (req, res) => {
  try {
    await obEnsureTable();
    const o = obClean(req.body || {});
    const keys = Object.keys(o); if (!keys.length) return res.status(400).json({ ok: false, error: 'nothing to update' });
    const sets = keys.map((k, i) => `${k}=$${i + 2}`).join(', ');
    const r = await pool.query(`UPDATE onboarding_clients SET ${sets}, updated_at=NOW() WHERE id=$1 RETURNING *`, [req.params.id, ...keys.map(k => o[k])]);
    if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, client: r.rows[0] });
  } catch (e) { res.status(e.message.startsWith('stage must') ? 400 : 500).json({ ok: false, error: e.message }); }
});
app.post('/onboarding/:id/note', requireKey, async (req, res) => {
  try {
    await obEnsureTable();
    const note = String((req.body && req.body.note) || '').trim();
    if (!note) return res.status(400).json({ ok: false, error: 'note is required' });
    const stamp = new Date().toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles', month: 'short', day: 'numeric', year: 'numeric' });
    const r = await pool.query(`UPDATE onboarding_clients SET notes = COALESCE(NULLIF(notes,''),'') || CASE WHEN COALESCE(notes,'')='' THEN '' ELSE E'\n' END || $2, updated_at=NOW() WHERE id=$1 RETURNING *`, [req.params.id, stamp + ': ' + note]);
    if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, client: r.rows[0] });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.delete('/onboarding/:id', requireKey, async (req, res) => {
  try {
    await obEnsureTable();
    const r = await pool.query('UPDATE onboarding_clients SET archived=true, updated_at=NOW() WHERE id=$1 RETURNING id', [req.params.id]);
    if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, archived: Number(r.rows[0].id) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
console.log('[Onboarding] tracker store ready - GET/POST /onboarding, PUT /onboarding/:id, POST /onboarding/:id/note');
// ─── end patch 353 ────────────────────────────────────────────────────────────
'''
shutil.copyfile(FILE, FILE + ".pre-353.bak")
open(FILE, "w", encoding="utf-8").write(src.replace(ANCHOR, ANCHOR + "\n" + BLOCK, 1))
print("Patched OK: %s" % FILE)
