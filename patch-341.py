#!/usr/bin/env python3
"""
patch-341.py  --  invoice-sheet-only billables, with real publisher cost,
                  plus a one-time baseline import

WHAT KYLER ASKED FOR
Record signed cases on the invoice sheet so what buyers owe is up to date,
WITHOUT touching any lead or changing what any publisher sees on their portal.
And include the publisher cost per pub so the P&L is correct.

WHY THE EXISTING ROUTE COULD NOT DO IT
/billable-queue/manual requires publisher_sub, writes no buyer into raw (so
/funding-summary reports the row under "Unknown" - that is where the existing
Unknown $2,500 row came from), and when approved with a lead_id it writes
leads.billable and leads.revenue, which is exactly what shows on a portal.

WHAT THIS ADDS
  POST /invoice-items  - creates a billable_queue row with lead_id NULL and the
  buyer recorded in raw, tagged raw.invoice_only = true. With no lead attached
  nothing reads back to a lead, so no portal can change.

  De-duplicated on (last 10 digits of phone, buyer), so re-pasting a list or
  redeploying cannot double what a buyer is shown to owe.

PUBLISHER COST
A hand-entered row has no lead, so it has no publisher and no payout, and
counting its revenue at zero cost would report it as 100% profit and inflate
the P&L. So on insert this looks the phone up in `leads` - a SELECT, nothing
written - and records that lead's publisher_sub on the invoice row. The normal
publisher_campaigns payout rate then applies and the P&L is correct.

Reading a lead changes nothing about it and nothing on any portal.

Where no lead matches the phone, the publisher is genuinely unknown, so that
row still counts in what the buyer owes but stays out of P&L and margin rather
than being counted at zero cost. The response reports those separately as
`manual_only.no_cost` so the gap is visible instead of silent.

ONE-TIME BASELINE IMPORT
Inserts the 7 cases Kyler confirmed on 2026-10-20, idempotently:

  NLD CPA @ $2,250   Graves, Daley, Ziegert, Priolo, Edwards      = $11,250
  LT-Intake @ $2,500 Loukia Altenor, Trudy Halverson              =  $5,000
                                                            total = $16,250

Only rows marked Signed were taken. The other 18 on that list were Unresponsive,
Not qualified, Chase, Pending Signature or Waiting for information, so they are
not money owed and are not imported.

No lead row is written anywhere in this patch.

Backup: server.js.pre-341.bak    Safe to re-run: refuses twice.
"""
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None


ROOT = find_root() or sys.exit("run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()

if "patch 341" in src:
    sys.exit("patch 341 already applied - nothing to do")
if "patch 338" not in src:
    sys.exit("patch 338 is not applied - run patch-338.py first")

ROUTE_ANCHOR = "app.get('/billable-queue', requireKey, async (req, res) => {"
MVA_ANCHOR = "function fsIsMva(campaign) { return FS_MVA_SIDE.indexOf(fsVertical(campaign)) > -1; }"
PNL_ANCHOR = "      /* ── P&L: earned on the signed date, approved and paid only ── */"
LANE_ANCHOR = "      const lk = pub + ' \u2192 ' + r.buyer;"
FLOAT_ANCHOR = "      const f = fsDays(r.lead_at, r.signed_at);"

for name, a in (("route", ROUTE_ANCHOR), ("mva helper", MVA_ANCHOR), ("pnl", PNL_ANCHOR),
                ("lane", LANE_ANCHOR), ("float", FLOAT_ANCHOR)):
    if src.count(a) != 1:
        sys.exit("%s anchor found %d times, expected 1 - aborting, nothing changed" % (name, src.count(a)))

# 1. A lead-less invoice row carries its own vertical, since it has no campaign.
src = src.replace(MVA_ANCHOR, MVA_ANCHOR + """
/* patch 341: a hand-entered invoice row has no lead and therefore no campaign,
   so it carries its own vertical in raw. */
function fsRowIsMva(r) {
  if (r && r.invoice_only) return String(r.vertical || 'mva').toLowerCase() !== 'ssdi';
  return fsIsMva(r ? r.campaign : null);
}""", 1)

# 2. Read the new fields and filter with the row-aware helper.
src = src.replace(
    "              l.received_at AS lead_at\n         FROM billable_queue bq",
    "              l.received_at AS lead_at,\n"
    "              (bq.raw->>'invoice_only' = 'true') AS invoice_only,\n"
    "              bq.raw->>'vertical' AS vertical, bq.raw->>'name' AS manual_name\n"
    "         FROM billable_queue bq", 1)
src = src.replace("    const rows = all.rows.filter(r => fsIsMva(r.campaign));",
                  "    const rows = all.rows.filter(fsRowIsMva);", 1)

# 3. Count hand-entered rows, and keep only the cost-less ones out of P&L/margin.
src = src.replace(LANE_ANCHOR,
    "      /* A hand-entered row only belongs in P&L and margin once its publisher\n"
    "         cost is known; at zero cost it would read as 100% profit. */\n"
    "      const fsCostKnown = !r.invoice_only || (!!r.publisher_sub && rate > 0);\n"
    "      if (r.invoice_only) {\n"
    "        manualOnly.count++; manualOnly.total += amt;\n"
    "        if (!fsCostKnown) { manualOnly.no_cost++; manualOnly.no_cost_total += amt; }\n"
    "      }\n\n"
    "      if (fsCostKnown) {\n"
    "      " + LANE_ANCHOR, 1)
src = src.replace(FLOAT_ANCHOR, "      }\n\n" + FLOAT_ANCHOR, 1)
src = src.replace(PNL_ANCHOR, "      if (!fsCostKnown) return;\n" + PNL_ANCHOR, 1)

# 4. Declare the accumulator and report it.
src = src.replace(
    "    let paidWindowTotal = 0;",
    "    const manualOnly = { count: 0, total: 0, no_cost: 0, no_cost_total: 0 };\n"
    "    let paidWindowTotal = 0;", 1)
src = src.replace(
    "      margins,\n      conversion,",
    "      manual_only: {\n"
    "        count: manualOnly.count, total: +manualOnly.total.toFixed(2),\n"
    "        no_cost: manualOnly.no_cost, no_cost_total: +manualOnly.no_cost_total.toFixed(2),\n"
    "        note: manualOnly.no_cost\n"
    "          ? (manualOnly.no_cost + ' hand-entered case(s) have no matching lead, so no publisher cost is known. They count in what buyers owe but are left out of P&L and margin.')\n"
    "          : 'Hand-entered rows have their publisher resolved, so they are included in P&L.'\n"
    "      },\n      margins,\n      conversion,", 1)

ROUTE = r'''// ══════════════════════════════════════════════════════
//  patch 341: invoice-sheet-only billables
//  billable_queue rows with lead_id NULL. Leads are read to resolve the
//  publisher (so P&L has a real cost) but never written, so no portal changes.
// ══════════════════════════════════════════════════════

/* A deploy can briefly run two instances, and two concurrent imports would each
   see "not present" and both insert. A unique index makes that impossible at the
   database rather than relying on the check-then-write above it. */
async function initInvoiceItemsGuard() {
  try {
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_bq_invoice_only_once
        ON billable_queue (RIGHT(regexp_replace(cid,'\\D','','g'),10), (raw->>'buyer'))
        WHERE raw->>'invoice_only' = 'true';
    `);
    console.log('[Invoice Items] duplicate guard ready');
  } catch (err) { console.error('[Invoice Items] guard init failed:', err.message); }
}

/* Find which publisher sent this phone number. SELECT only. */
async function fsPublisherForPhone(phone) {
  try {
    const r = await pool.query(
      `SELECT publisher_sub, campaign FROM leads
        WHERE RIGHT(regexp_replace(COALESCE(phone,''),'\\D','','g'),10) = $1
          AND COALESCE(NULLIF(TRIM(publisher_sub),''),'') <> ''
        ORDER BY received_at DESC LIMIT 1`, [phone]);
    return r.rows[0] || null;
  } catch (err) { return null; }
}

app.post('/invoice-items', requireKey, async (req, res) => {
  try {
    const b = req.body || {};
    const items = Array.isArray(b.items) ? b.items : [b];
    if (!items.length) return res.status(400).json({ ok: false, error: 'no items' });

    const added = [], skipped = [];
    for (const it of items) {
      const buyer = String(it.buyer || '').trim();
      const phone = String(it.cid || it.phone || '').replace(/\D/g, '').slice(-10);
      const amt = Number(it.amount);
      if (!buyer)              { skipped.push({ item: it, why: 'buyer required' }); continue; }
      if (phone.length !== 10) { skipped.push({ item: it, why: 'cid must be a 10 digit phone' }); continue; }
      if (!(amt > 0))          { skipped.push({ item: it, why: 'amount must be positive' }); continue; }

      /* The same phone and buyer twice is a double count of what is owed. */
      const dup = await pool.query(
        `SELECT id, status FROM billable_queue
          WHERE RIGHT(regexp_replace(cid,'\\D','','g'),10) = $1
            AND COALESCE(NULLIF(TRIM(raw->>'buyer'),''),'') = $2 LIMIT 1`,
        [phone, buyer]);
      if (dup.rows.length) { skipped.push({ cid: phone, buyer, why: 'already recorded', existing: dup.rows[0] }); continue; }

      /* Resolve the publisher from the lead so the P&L carries a real cost. */
      const match = it.publisher_sub ? null : await fsPublisherForPhone(phone);
      const pub = it.publisher_sub || (match && match.publisher_sub) || null;

      try {
      const r = await pool.query(
        `INSERT INTO billable_queue (cid, amount, publisher_sub, lead_id, status, raw)
         VALUES ($1,$2,$3,NULL,'approved',$4::jsonb) RETURNING id`,
        [phone, amt, pub, JSON.stringify({
          buyer: buyer, invoice_only: true, vertical: String(it.vertical || 'mva'),
          source: 'invoice_manual', name: it.name || null, note: it.note || null,
          publisher_resolved_from: match ? 'lead_phone_match' : (it.publisher_sub ? 'supplied' : null),
          added_at: new Date().toISOString()
        })]);
      added.push({ id: r.rows[0].id, cid: phone, buyer, amount: amt, name: it.name || null, publisher_sub: pub });
      } catch (e) {
        if (e && e.code === '23505') { skipped.push({ cid: phone, buyer, why: 'already recorded' }); continue; }
        throw e;
      }
    }
    const total = added.reduce((s, a) => s + a.amount, 0);
    const noPub = added.filter(a => !a.publisher_sub).length;
    console.log('[Invoice Items] +' + added.length + ' row(s), $' + total +
                ', ' + noPub + ' without a publisher, ' + skipped.length + ' skipped');
    return res.json({ ok: true, added, skipped, count: added.length,
                      total: +total.toFixed(2), without_publisher: noPub });
  } catch (err) {
    console.error('[invoice-items]', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

/* One-time baseline Kyler confirmed 2026-10-20. Only rows marked Signed.
   Idempotent on cid+buyer, so a redeploy cannot double what a buyer owes. */
async function fsSeedBaseline20261020() {
  const seed = [
    { buyer: 'NLD CPA',   amount: 2250, cid: '3149130092', name: 'Graves' },
    { buyer: 'NLD CPA',   amount: 2250, cid: '5615814335', name: 'Daley' },
    { buyer: 'NLD CPA',   amount: 2250, cid: '2692172777', name: 'Ziegert' },
    { buyer: 'NLD CPA',   amount: 2250, cid: '8315215562', name: 'Priolo' },
    { buyer: 'NLD CPA',   amount: 2250, cid: '5619058517', name: 'Edwards' },
    { buyer: 'LT-Intake', amount: 2500, cid: '5516559276', name: 'Loukia Altenor' },
    { buyer: 'LT-Intake', amount: 2500, cid: '2533311718', name: 'Trudy Halverson' }
  ];
  try {
    let added = 0, noPub = 0;
    for (const s of seed) {
      const dup = await pool.query(
        `SELECT 1 FROM billable_queue
          WHERE RIGHT(regexp_replace(cid,'\\D','','g'),10) = $1
            AND COALESCE(NULLIF(TRIM(raw->>'buyer'),''),'') = $2 LIMIT 1`,
        [s.cid, s.buyer]);
      if (dup.rows.length) continue;
      const match = await fsPublisherForPhone(s.cid);
      const pub = match ? match.publisher_sub : null;
      if (!pub) noPub++;
      try {
      await pool.query(
        `INSERT INTO billable_queue (cid, amount, publisher_sub, lead_id, status, raw)
         VALUES ($1,$2,$3,NULL,'approved',$4::jsonb)`,
        [s.cid, s.amount, pub, JSON.stringify({
          buyer: s.buyer, invoice_only: true, vertical: 'mva', source: 'baseline_2026_10_20',
          name: s.name, note: 'Signed; baseline import 2026-10-20',
          publisher_resolved_from: match ? 'lead_phone_match' : null,
          added_at: new Date().toISOString()
        })]);
      added++;
      } catch (e) { if (!(e && e.code === '23505')) throw e; }
    }
    if (added) console.log('[Invoice Baseline] imported ' + added + ' signed case(s), ' + noPub + ' without a publisher');
    else console.log('[Invoice Baseline] already present, nothing imported');
  } catch (err) { console.error('[Invoice Baseline] failed:', err.message); }
}
initInvoiceItemsGuard().then(fsSeedBaseline20261020);

'''

src = src.replace(ROUTE_ANCHOR, ROUTE + ROUTE_ANCHOR, 1)

shutil.copyfile(FILE, FILE + ".pre-341.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 341 applied")
print("  + POST /invoice-items (lead_id NULL; no portal can change)")
print("  + publisher resolved from leads by phone, read-only, so P&L has real cost")
print("  + one-time baseline: 5 NLD @ $2,250 + 2 LT-Intake @ $2,500 = $16,250")
print("backup at server.js.pre-341.bak")
