#!/usr/bin/env python3
"""
patch-320.py  --  KramMarketing MVA transfers auto-ingest + portal-safe dispo

WHAT IT CHANGES  (krw-backend/server.js only)

  The LT buyer-sheet scanner already reads the dialer-export tab (that is how
  the "no matching lead" phones surfaced). This teaches it to recognize the
  rows marked "KramMarketing" in the security_phrase column and file them as
  Joshua Duran's MVA transfers, onto a standalone publisher line
  (KRW-JOSHUA-MVA) that never mixes with his SSDI leads.

  1. bsMapHeader also locates security_phrase / state / email columns.
  2. Each scanned row now carries secphrase / state / email.
  3. In the unmatched branch, a row whose security_phrase says "kram" is
     create-or-updated as a KRW-JOSHUA-MVA lead, with the VICIdial status
     code mapped to a human disposition (A -> answering machine, NQ ->
     Rejected: not qualified, ATTY -> Rejected: attorney represented, etc).
     Noah's normal LT matching is untouched - this only fires when a row
     matched no existing lead AND is flagged KramMarketing.
  4. /leads/feed exposes disp_status / disp_note (from buyer_disposition).
     These carry no buyer identity, so they survive the portal strip and let
     Josh's MVA box show the real disposition, not just a status code.

  Idempotent on the data: re-scans confirm unchanged transfers (stamp
  confirmed_at) and only rewrite a disposition when it actually changed.

PAIR WITH patch 319 (portal) and seed-josh-mva.sql. Deploy order:
    1. this patch, let Railway deploy
    2. run seed-josh-mva.sql
    3. patch 319 on ssdi-portal, commit + push

Backup: server.js.pre-320.bak    Safe to re-run: refuses twice.
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
if not os.path.exists(FILE):
    sys.exit("krw-backend/server.js not found at " + FILE)

src = open(FILE, encoding="utf-8").read()
if "bsUpsertKramMva" in src or "patch 320" in src:
    sys.exit("patch 320 already applied to server.js - nothing to do")

def apply(s, anchor, replacement, what):
    n = s.count(anchor)
    if n != 1:
        sys.exit("ABORT (%s): anchor found %d times, expected exactly 1. No files changed." % (what, n))
    return s.replace(anchor, replacement, 1)

# ── 1. bsMapHeader: also find security_phrase / state / email ───────────────
A1 = "billable: find(/^BILLABLE$/), signed: find(/^SIGNED$/) };"
R1 = ("billable: find(/^BILLABLE$/), signed: find(/^SIGNED$/), "
      "secphrase: find(/^SECURITY[_ ]?PHRASE$/, /SECURITY.?PHRASE/), "
      "state: find(/^(STATE|PROVINCE)$/), "
      "email: find(/^(EMAIL|E-?MAIL|EMAIL ADDRESS)$/, /EMAIL/) };")
src = apply(src, A1, R1, "bsMapHeader security_phrase/state/email")

# ── 2. carry secphrase / state / email onto each scanned row ────────────────
A2 = "          name: map.name > -1 ? bsNorm(r[map.name]) : [bsNorm(r[map.first]), bsNorm(r[map.last])].filter(Boolean).join(' '),\n        };"
R2 = ("          name: map.name > -1 ? bsNorm(r[map.name]) : [bsNorm(r[map.first]), bsNorm(r[map.last])].filter(Boolean).join(' '),\n"
      "          secphrase: map.secphrase > -1 ? bsNorm(r[map.secphrase]) : '', state: map.state > -1 ? bsNorm(r[map.state]) : '', email: map.email > -1 ? bsNorm(r[map.email]) : '',\n"
      "        };")
src = apply(src, A2, R2, "row carries secphrase/state/email")

# ── 3. unmatched branch: ingest KramMarketing rows ──────────────────────────
A3 = "        if (!lead) { rep.unmatched++; continue; }"
R3 = ("        if (!lead) {\n"
      "          rep.unmatched++;\n"
      "          // patch 320: KramMarketing MVA transfers -> Josh's standalone MVA line\n"
      "          if (/kram/i.test(r.secphrase || '')) {\n"
      "            try { await bsUpsertKramMva(client, r); rep.kram = (rep.kram || 0) + 1; }\n"
      "            catch (e) { console.error('[Buyer Sheets] KramMVA upsert failed for', r.phone, e.message); }\n"
      "          }\n"
      "          continue;\n"
      "        }")
src = apply(src, A3, R3, "unmatched KramMarketing ingest")

# ── 4. the helpers, inserted just before bsScanOne ──────────────────────────
A4 = "// ── scan ──\nasync function bsScanOne(cfg, trigger) {"
HELPERS = r'''// ── patch 320: KramMarketing MVA transfers (Josh's standalone line) ──────────
const KRAM_MVA_PUB = 'KRW-JOSHUA-MVA';
function kramClassify(r) {
  const code = String(r.status || '').toUpperCase().trim();
  const note = (r.notes || '').trim();
  const REJECT = { NQ:'Not qualified', NI:'Not interested', ATTY:'Attorney represented', DNC:'Do not call', LANGBA:'Language barrier', WN:'Wrong number' };
  const OPENC  = { A:'Answering machine', DAIR:'Dead air', NANQUE:'No answer', NA:'No answer', B:'Busy', N:'New', NEW:'New', CALLBK:'Callback' };
  if (REJECT[code]) return { kind:'rejected', status:'Rejected', note:'Rejected — ' + REJECT[code] + (note ? ': ' + note : '') };
  if (OPENC[code])  return { kind:'open',     status:'Open — in outreach', note:'Open — ' + OPENC[code].toLowerCase() + (note ? ': ' + note : '') };
  // unknown VICIdial code: keep it open so Josh keeps working it, carry the reason
  return { kind:'open', status:'Open — in outreach', note:'Open — ' + (note || code || 'in outreach') };
}
async function bsUpsertKramMva(client, r) {
  if (!r.phone) return 'skipped';
  const cls = kramClassify(r);
  const nm = String(r.name || '').trim().split(/\s+/).filter(Boolean);
  const first = nm.length ? nm[0] : '';
  const last  = nm.length > 1 ? nm.slice(1).join(' ') : '';
  const recv  = r.date || new Date().toISOString();
  const leadStatus = cls.kind === 'rejected' ? 'buyer_rejected' : 'forwarded';
  // Josh's MVA publisher line (idempotent - its own portal_id keeps SSDI separate)
  await client.query(
    `INSERT INTO publishers (pub_id, name, portal_id, active) VALUES ($1,$2,$1,true) ON CONFLICT (pub_id) DO NOTHING`,
    [KRAM_MVA_PUB, 'Joshua Duran — MVA Transfers']);
  const existing = (await client.query(
    `SELECT id, raw FROM leads WHERE publisher_sub=$1 AND RIGHT(regexp_replace(phone,'\D','','g'),10)=$2 ORDER BY received_at DESC LIMIT 1`,
    [KRAM_MVA_PUB, r.phone])).rows[0];
  if (!existing) {
    await client.query(
      `INSERT INTO leads (received_at, campaign, vertical, status, first_name, last_name, email, phone, state, buyer_status, notes, publisher_sub, raw)
       VALUES ($1::timestamptz,'mva-transfer','MVA',$2::text,$3,$4,$5,$6,$7,$8::text,$9::text,$10,
         jsonb_build_object('buyer_name','LT-Intake-transfer','source','josh_mva_transfer','transfer_source','KramMarketing','lt_dialer_status',$11::text,
           'buyer_disposition', jsonb_build_object('source','lt_transfer','status',$8::text,'note',$9::text,'sheet_status',$11::text,'synced_at',NOW(),'confirmed_at',NOW())))`,
      [recv, leadStatus, first, last, r.email || '', r.phone, r.state || '', cls.status, cls.note, KRAM_MVA_PUB, String(r.status || '')]);
    return 'inserted';
  }
  const prev = (existing.raw && existing.raw.buyer_disposition) || {};
  if (prev.status === cls.status && prev.note === cls.note) {
    await client.query(`UPDATE leads SET raw = jsonb_set(COALESCE(raw,'{}'::jsonb), '{buyer_disposition,confirmed_at}', to_jsonb(NOW()), true) WHERE id=$1::int`, [existing.id]);
    return 'confirmed';
  }
  await client.query(
    `UPDATE leads SET status=$1::text, buyer_status=$2::text, notes=$3::text,
       raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('lt_dialer_status',$5::text,
         'buyer_disposition', jsonb_build_object('source','lt_transfer','status',$2::text,'note',$3::text,'sheet_status',$5::text,'synced_at',NOW(),'confirmed_at',NOW()))
     WHERE id=$4::int`,
    [leadStatus, cls.status, cls.note, existing.id, String(r.status || '')]);
  return 'updated';
}

// ── scan ──
async function bsScanOne(cfg, trigger) {'''
src = apply(src, A4, HELPERS, "KramMVA helpers")

# ── 5. surface kram count in the scan log ───────────────────────────────────
A5 = "${rep.updated} updated, ${rep.queued} queued`}`);"
R5 = "${rep.updated} updated, ${rep.queued} queued${rep.kram ? ', ' + rep.kram + ' kram-mva' : ''}`}`);"
src = apply(src, A5, R5, "scan log kram count")

# ── 6. expose disp_status / disp_note on the feed (survives portal strip) ────
A6 = ("              raw->>'incident_date' as incident_date, raw->>'county' as county\n"
      "       FROM leads ${wc} ORDER BY received_at DESC LIMIT $${i}`, params);")
R6 = ("              raw->>'incident_date' as incident_date, raw->>'county' as county,\n"
      "              raw->'buyer_disposition'->>'status' as disp_status,\n"
      "              raw->'buyer_disposition'->>'note'   as disp_note\n"
      "       FROM leads ${wc} ORDER BY received_at DESC LIMIT $${i}`, params);")
src = apply(src, A6, R6, "feed disp_status/disp_note")

shutil.copyfile(FILE, FILE + ".pre-320.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 320 applied to", FILE)
print("backup:", FILE + ".pre-320.bak")
print("Commit + push krw-backend, let Railway deploy, then run seed-josh-mva.sql.")
