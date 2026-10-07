#!/usr/bin/env python3
"""
patch-301.py  --  POST /leads/:id/resend-ladder: re-offer a stored lead to its
                  buyer ladder without creating a new lead record

WHY
  Three times today a lead got stuck (TF gate, NLD format bug, 003 cap) and
  the only way to retry it was re-POSTing the whole payload to the endpoint -
  which creates a NEW lead id each time and, since patch 270 made TrustedForm
  certs single-use, now gets rejected as a reused cert the moment the stored
  lead already carries one. The janitor can't help either: its candidate query
  only picks up state-cap rejects and NLD code-1028 rejects.

WHAT IT ADDS
  POST /leads/:id/resend-ladder   (admin key or dashboard login)

  Re-runs the buyer ladder on the EXISTING lead record - same id, same cert,
  no new row. Reuses janSend(), so the payloads are identical to what the
  overnight janitor sends (incident_date normalization from patch 298
  included).

  Rules mirrored from the live ladder:
    - CA/CO blocked, always
    - intake states -> CH-Intake / LT-Intake (fewer-today first)
    - NLD states    -> NLD CPA, 10/day cap respected
    - everything else / all declined:
        Leadbloom  -> CH / LT again without the state filter (patch 299's
                      overflow rung; 003 NEVER, per Kyler Oct 7)
        other pubs -> MVA-003-LT, 5/day cap respected
  A lead that is already forwarded or billable is refused (409) so nobody can
  double-send a sold lead. Every attempt is recorded on the lead's raw JSON.

Usage (from ~/krw/krw-backend):
    python3 patch-301.py
    node --check server.js
    git add -A && git commit -m "patch 301: manual resend-ladder route" && git push

Backup: server.js.pre-301.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 301" in src:
    sys.exit("patch 301 already applied - nothing to do")

ANCHOR = "console.log('[Janitor] armed - daily at', JANITOR_TIME_ET, 'ET (manual: POST /janitor/run)');"
if src.count(ANCHOR) != 1:
    sys.exit("could not find the janitor tail - aborting, server.js untouched")

ROUTE = ANCHOR + """

// patch 301: re-offer one stored lead to its ladder, in place. No new lead
// row, no second TrustedForm use - the record keeps its id and its cert.
app.post('/leads/:id/resend-ladder', requireKey, async (req, res) => {
  try {
    const q = await pool.query('SELECT * FROM leads WHERE id=$1', [req.params.id]);
    if (!q.rows[0]) return res.status(404).json({ ok: false, error: 'Lead not found' });
    const row = q.rows[0];
    if (row.status === 'forwarded') return res.status(409).json({ ok: false, error: 'Lead is already forwarded - refusing to double-send' });
    if (row.billable === true) return res.status(409).json({ ok: false, error: 'Lead is billable - refusing to touch it' });
    if ((row.vertical || '') !== 'MVA') return res.status(400).json({ ok: false, error: 'Only MVA ladder leads can be re-sent here' });

    const b = row.raw || {}; b.phone = b.phone || row.phone;
    const st = janState(row.state);
    if (st === 'CA' || st === 'CO') return res.status(400).json({ ok: false, error: st + ' is blocked company-wide' });

    const pub = row.publisher_sub || '';
    const isLeadbloom = pub === 'KRW-LEADBLOOM-MVA';
    const nldToday = await janCountToday('NLD CPA');
    const lt003Today = await janCountToday('MVA-003-LT');
    const chToday = await janCountToday('CH-Intake');
    const ltToday = await janCountToday('LT-Intake');

    const rungs = [];
    const pushIntake = () => {
      if (process.env.LT_INTAKE_PASS && ltToday < chToday) rungs.push('LT-Intake', 'CH-Intake');
      else { rungs.push('CH-Intake'); if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake'); }
    };
    if (JAN_INTAKE_STATES.includes(st)) pushIntake();
    if (NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10) rungs.push('NLD CPA');
    if (isLeadbloom) {
      // patch 299 rule: Leadbloom never goes to 003. Out-of-state leads fall
      // back to CH/LT without the state filter instead.
      if (!JAN_INTAKE_STATES.includes(st)) pushIntake();
    } else if (lt003Today < 5) {
      rungs.push('MVA-003-LT');
    }
    const plan = rungs.filter((x, i) => rungs.indexOf(x) === i);   // dedupe, order kept
    if (!plan.length) return res.status(409).json({ ok: false, error: 'No eligible buyer right now (caps reached)', state: st });

    const attempts = [];
    for (const buyer of plan) {
      let out;
      try { out = await janSend(buyer, b, st, row.id, pub); }
      catch (e) { out = { result: { error: e.message }, accepted: false }; }
      attempts.push({ buyer, accepted: out.accepted, response: out.result });
      console.log(`[Resend-Ladder] ${out.accepted ? '\\u2713' : '\\u2715'} ${buyer} | lead ${row.id} | ${st}`);
      if (out.accepted) {
        await pool.query(
          `UPDATE leads SET status='forwarded', buyer_status='Accepted', buyer_error=NULL,
             buyer_response = COALESCE(buyer_response,'{}'::jsonb) || $1::jsonb,
             raw = COALESCE(raw,'{}'::jsonb) || $2::jsonb WHERE id=$3`,
          [JSON.stringify({ resend_final: out.result, resend_attempts: attempts }),
           JSON.stringify({ buyer_name: buyer, manual_resend: { at: new Date().toISOString(), buyer } }), row.id]);
        return res.json({ ok: true, result: 'success', buyer, krw_id: row.id, attempts: attempts.map(a => a.buyer + (a.accepted ? ':accepted' : ':rejected')) });
      }
    }
    await pool.query(`UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || $1::jsonb WHERE id=$2`,
      [JSON.stringify({ manual_resend: { at: new Date().toISOString(), note: 'no buyer accepted', attempts } }), row.id]);
    return res.json({ ok: false, result: 'rejected', krw_id: row.id, attempts: attempts.map(a => a.buyer + ':rejected'), detail: attempts });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});"""

src = src.replace(ANCHOR, ROUTE, 1)

# the route needs NLD's state list in scope at module level - the ladder's copy
# lives inside the request handler. Reuse the janitor's file-level pattern.
JAN_ANCHOR = "const JAN_NLD_STATES    = ['UT','MT','WY','AZ','NV','OK','NE','ND','IA','NM'];"
if src.count(JAN_ANCHOR) != 1:
    sys.exit("could not find JAN_NLD_STATES - aborting, server.js untouched")
src = src.replace(JAN_ANCHOR, JAN_ANCHOR + """
// patch 301: NLD's full current list (includes CA for completeness even though
// CA is blocked upstream) for the manual resend route.
const NLD_ONLY_STATES_GLOBAL = ['UT','MT','WY','AZ','CA','NV','OK','NE','ND','IA','NM'];""", 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-301.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 301 applied. backup: server.js.pre-301.bak")
print("  POST /leads/:id/resend-ladder - re-offers a stored lead, same id, same cert")
print("  Leadbloom: never 003. Others: 003 only under its 5/day cap.")
print("")
print("next: node --check server.js && git add -A && git commit -m 'patch 301' && git push")
