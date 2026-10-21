#!/usr/bin/env python3
"""
patch-336.py  --  MVA invoice tracking + GET /funding-summary

THE PROBLEM
billable_queue goes pending -> approved -> paid with no way to record that an
invoice was actually SENT. A signed case you have not invoiced and one you
invoiced three weeks ago that the buyer is sitting on look identical. That is
why the 8 signed NLD cases are invisible.

Separately, the Invoicing tab calls /invoice-summary, which filters on
source_system='trackdrive' (the SSDI call billing). The whole MVA side lives in
billable_queue and never appears there.

WHAT THIS ADDS
  1. Four columns on billable_queue: invoice_id, invoiced_at, due_date,
     terms_days. Invoice state is ORTHOGONAL to status, so mark-paid and every
     existing approval path keep working untouched:
        approved + invoiced_at NULL     = signed, needs invoicing
        approved + invoiced_at NOT NULL = invoiced, waiting on the buyer
        paid                            = settled
  2. GET  /funding-summary          (read-only)
  3. POST /billable-queue/invoice   (groups a buyer's uninvoiced cases)

MVA ONLY. SSDI is excluded using the same campaign-to-vertical rules the
dashboard already uses (lxVertical), so nothing mixes the two.

NOTES ON THE NUMBERS
  - Signed date is approximated by billable_queue.received_at, which is when the
    sheet scan found the case. raw->>'sheet_date' is free text like "9/22/2026"
    and is not safe to cast, so float days are accurate to about a scan window.
  - Publisher payout rates are loaded into a map and applied in JS. Joining
    publisher_campaigns in SQL fans out when a publisher has several campaigns.
  - Nothing here moves money or contacts a buyer. Invoicing marks our own rows.

TRADE-OFF
  due_date is stored as a plain DATE computed from terms_days at invoice time.
  Changing a buyer's terms later does not retroactively move existing due dates,
  which is the behaviour you want for an invoice already sent.

Backup: server.js.pre-336.bak    Safe to re-run: refuses twice.
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

if "patch 336" in src:
    sys.exit("patch 336 already applied - nothing to do")

SCHEMA_ANCHOR = "initPortalPostbacks();"
ROUTE_ANCHOR = "app.get('/billable-queue', requireKey, async (req, res) => {"

for name, anchor in (("schema", SCHEMA_ANCHOR), ("route", ROUTE_ANCHOR)):
    if src.count(anchor) != 1:
        sys.exit("%s anchor found %d times, expected 1 - aborting, nothing changed"
                 % (name, src.count(anchor)))

SCHEMA_BLOCK = r'''
// ══════════════════════════════════════════════════════
//  patch 336: invoice tracking on billable_queue
//  Orthogonal to status, so mark-paid and the approval paths are unaffected.
// ══════════════════════════════════════════════════════
async function initInvoiceTracking() {
  try {
    await pool.query(`
      ALTER TABLE billable_queue ADD COLUMN IF NOT EXISTS invoice_id  TEXT;
      ALTER TABLE billable_queue ADD COLUMN IF NOT EXISTS invoiced_at TIMESTAMPTZ;
      ALTER TABLE billable_queue ADD COLUMN IF NOT EXISTS due_date    DATE;
      ALTER TABLE billable_queue ADD COLUMN IF NOT EXISTS terms_days  INTEGER;
      CREATE INDEX IF NOT EXISTS idx_bq_invoice ON billable_queue (invoice_id);
    `);
    console.log('[Invoice Tracking] schema ready');
  } catch (err) { console.error('[Invoice Tracking] schema init failed:', err.message); }
}
initInvoiceTracking();
'''

ROUTE_BLOCK = r'''// ══════════════════════════════════════════════════════
//  patch 336: MVA funding + invoicing
// ══════════════════════════════════════════════════════

// Mirrors lxVertical() in the dashboard so server and UI agree on what is MVA.
function fsVertical(campaign) {
  const c = String(campaign || '').toLowerCase();
  if (c.indexOf('mva') > -1) return 'mva';
  if (c.indexOf('ride') > -1 || c.indexOf('lyft') > -1 || c.indexOf('uber') > -1) return 'rideshare';
  if (c.indexOf('roblox') > -1) return 'roblox';
  if (c.indexOf('roundup') > -1) return 'roundup';
  if (c.indexOf('ssdi') > -1) return 'ssdi';
  return 'other';
}
const FS_MVA_SIDE = ['mva', 'rideshare', 'roblox', 'roundup'];
function fsIsMva(campaign) { return FS_MVA_SIDE.indexOf(fsVertical(campaign)) > -1; }

function fsNum(v) { const n = parseFloat(v); return isFinite(n) ? n : 0; }
function fsDays(a, b) {
  if (!a || !b) return null;
  const d = (new Date(b) - new Date(a)) / 86400000;
  return isFinite(d) ? d : null;
}
function fsToday() { const d = new Date(); d.setHours(0, 0, 0, 0); return d; }

// ── GET /funding-summary ──────────────────────────────
app.get('/funding-summary', requireKey, async (req, res) => {
  try {
    const weeks = Math.max(1, Math.min(52, parseInt(req.query.weeks || '8', 10) || 8));
    const today = fsToday();

    // Publisher payout rates, resolved in JS to avoid join fan-out.
    const rateRows = await pool.query(
      `SELECT pub_id, campaign, COALESCE(payout_rate,0)::float AS payout_rate
         FROM publisher_campaigns WHERE active = true`);
    const rateFor = (pub, campaign) => {
      if (!pub) return 0;
      const mine = rateRows.rows.filter(r => r.pub_id === pub);
      if (!mine.length) return 0;
      const exact = mine.find(r => r.campaign === campaign);
      return fsNum((exact || mine[0]).payout_rate);
    };

    // Every signed case with its lead, newest first.
    const all = await pool.query(
      `SELECT bq.id, bq.cid, bq.status, bq.amount::float AS amount, bq.publisher_sub,
              bq.received_at AS signed_at, bq.invoice_id, bq.invoiced_at,
              bq.due_date, bq.terms_days, bq.resolved_at,
              COALESCE(NULLIF(TRIM(bq.raw->>'buyer'),''),'Unknown') AS buyer,
              l.campaign, l.state, l.first_name, l.last_name,
              l.received_at AS lead_at
         FROM billable_queue bq
         LEFT JOIN leads l ON l.id = bq.lead_id
        WHERE bq.status IN ('approved','paid')
        ORDER BY bq.received_at DESC`);

    // MVA side only. SSDI and unclassified stay out entirely.
    const rows = all.rows.filter(r => fsIsMva(r.campaign));

    const readyMap = {}, invMap = {}, paidMap = {}, payMap = {}, laneMap = {}, floatMap = {};
    let paidWindowTotal = 0;
    const paidSince = new Date(today.getTime() - weeks * 7 * 86400000);

    rows.forEach(r => {
      const pub = r.publisher_sub || 'Unknown';
      const rate = rateFor(r.publisher_sub, r.campaign);
      const amt = fsNum(r.amount);

      // ── Ready to invoice: approved, never invoiced ──
      if (r.status === 'approved' && !r.invoiced_at) {
        const b = readyMap[r.buyer] || (readyMap[r.buyer] =
          { buyer: r.buyer, count: 0, amount: 0, oldest_days: 0, cases: [] });
        const age = fsDays(r.signed_at, today);
        b.count++;
        b.amount += amt;
        if (age !== null && age > b.oldest_days) b.oldest_days = Math.floor(age);
        b.cases.push({
          id: r.id, cid: r.cid, amount: amt, publisher_sub: pub,
          signed_at: r.signed_at, first_name: r.first_name, last_name: r.last_name,
          state: r.state, days_since_signed: age === null ? null : Math.floor(age)
        });
      }

      // ── Awaiting payment: invoiced, not yet paid ──
      if (r.status === 'approved' && r.invoiced_at) {
        const key = r.invoice_id || ('no-id-' + r.id);
        const v = invMap[key] || (invMap[key] = {
          invoice_id: r.invoice_id, buyer: r.buyer, count: 0, amount: 0,
          invoiced_at: r.invoiced_at, due_date: r.due_date,
          days_outstanding: 0, days_overdue: 0, overdue: false
        });
        v.count++;
        v.amount += amt;
        const out = fsDays(r.invoiced_at, today);
        v.days_outstanding = out === null ? 0 : Math.floor(out);
        if (r.due_date) {
          const over = fsDays(new Date(r.due_date), today);
          v.days_overdue = over !== null && over > 0 ? Math.floor(over) : 0;
          v.overdue = v.days_overdue > 0;
        }
      }

      // ── Paid inside the window ──
      if (r.status === 'paid') {
        const when = r.resolved_at || r.signed_at;
        if (when && new Date(when) >= paidSince) {
          const p = paidMap[r.buyer] || (paidMap[r.buyer] = { buyer: r.buyer, count: 0, amount: 0 });
          p.count++; p.amount += amt; paidWindowTotal += amt;
        }
      }

      // ── Payables: publisher cost on everything still owed to us ──
      if (r.status === 'approved') {
        const p = payMap[pub] || (payMap[pub] = { publisher: pub, count: 0, amount: 0 });
        p.count++; p.amount += rate;
      }

      // ── Margin per publisher -> buyer lane, across all signed history ──
      const lk = pub + ' → ' + r.buyer;
      const L = laneMap[lk] || (laneMap[lk] = {
        lane: lk, publisher: pub, buyer: r.buyer, vertical: fsVertical(r.campaign),
        count: 0, buyer_pays: 0, publisher_gets: 0
      });
      L.count++; L.buyer_pays += amt; L.publisher_gets += rate;

      // ── Float: lead arriving to case signed ──
      const f = fsDays(r.lead_at, r.signed_at);
      if (f !== null && f >= 0 && f <= 365) {
        const fm = floatMap[pub] || (floatMap[pub] = { publisher: pub, total: 0, count: 0 });
        fm.total += f; fm.count++;
      }
    });

    const ready_by_buyer = Object.values(readyMap)
      .map(b => ({ ...b, amount: +b.amount.toFixed(2) }))
      .sort((a, b) => b.amount - a.amount);
    const ready_total = ready_by_buyer.reduce((s, b) => s + b.amount, 0);

    const by_invoice = Object.values(invMap)
      .map(v => ({ ...v, amount: +v.amount.toFixed(2) }))
      .sort((a, b) => (b.days_overdue - a.days_overdue) || (b.days_outstanding - a.days_outstanding));
    const awaiting_total = by_invoice.reduce((s, v) => s + v.amount, 0);
    const overdue_total = by_invoice.filter(v => v.overdue).reduce((s, v) => s + v.amount, 0);
    const oldest_overdue = by_invoice.reduce((m, v) => Math.max(m, v.days_overdue || 0), 0);

    // Per-buyer rollup for the dashboard table.
    const buyers = {};
    const touch = (name) => buyers[name] || (buyers[name] = {
      buyer: name, ready: 0, ready_count: 0, invoiced_unpaid: 0,
      overdue: 0, paid_window: 0, total_owed: 0
    });
    ready_by_buyer.forEach(b => { const x = touch(b.buyer); x.ready = b.amount; x.ready_count = b.count; });
    by_invoice.forEach(v => {
      const x = touch(v.buyer);
      x.invoiced_unpaid += v.amount;
      if (v.overdue) x.overdue += v.amount;
    });
    Object.values(paidMap).forEach(p => { touch(p.buyer).paid_window = +p.amount.toFixed(2); });
    const by_buyer = Object.values(buyers).map(x => ({
      ...x,
      invoiced_unpaid: +x.invoiced_unpaid.toFixed(2),
      overdue: +x.overdue.toFixed(2),
      total_owed: +(x.ready + x.invoiced_unpaid).toFixed(2)
    })).sort((a, b) => b.total_owed - a.total_owed);

    const payables_by_publisher = Object.values(payMap)
      .map(p => ({ publisher: p.publisher, count: p.count, amount: +p.amount.toFixed(2) }))
      .sort((a, b) => b.amount - a.amount);
    const payables_total = payables_by_publisher.reduce((s, p) => s + p.amount, 0);

    const margins = Object.values(laneMap).map(l => ({
      lane: l.lane, publisher: l.publisher, buyer: l.buyer, vertical: l.vertical,
      count: l.count,
      buyer_pays_avg: +(l.buyer_pays / l.count).toFixed(2),
      publisher_gets_avg: +(l.publisher_gets / l.count).toFixed(2),
      margin_avg: +((l.buyer_pays - l.publisher_gets) / l.count).toFixed(2),
      margin_total: +(l.buyer_pays - l.publisher_gets).toFixed(2)
    })).sort((a, b) => b.margin_total - a.margin_total);

    const floatCases = Object.values(floatMap).reduce((s, f) => s + f.count, 0);
    const floatOverall = floatCases
      ? +(Object.values(floatMap).reduce((s, f) => s + f.total, 0) / floatCases).toFixed(1)
      : null;
    const float_by_publisher = Object.values(floatMap)
      .map(f => ({ publisher: f.publisher, days: +(f.total / f.count).toFixed(1), cases: f.count }))
      .sort((a, b) => b.cases - a.cases);

    // Conversion: MVA leads sent vs signed, inside the window.
    const conv = await pool.query(
      `SELECT COALESCE(NULLIF(TRIM(l.publisher_sub),''),'Unknown') AS publisher,
              COALESCE(l.campaign,'') AS campaign,
              COUNT(*)::int AS leads_sent,
              COUNT(*) FILTER (
                WHERE EXISTS (SELECT 1 FROM billable_queue b
                               WHERE b.lead_id = l.id AND b.status IN ('approved','paid'))
              )::int AS signed
         FROM leads l
        WHERE l.received_at >= NOW() - ($1::int * INTERVAL '1 week')
          AND COALESCE(l.raw->>'excluded','') <> 'true'
        GROUP BY 1,2`, [weeks]);

    const convMap = {};
    conv.rows.filter(r => fsIsMva(r.campaign)).forEach(r => {
      const c = convMap[r.publisher] || (convMap[r.publisher] =
        { publisher: r.publisher, leads_sent: 0, signed: 0 });
      c.leads_sent += r.leads_sent;
      c.signed += r.signed;
    });
    const conversion = Object.values(convMap).map(c => ({
      ...c, rate: c.leads_sent ? +((c.signed / c.leads_sent) * 100).toFixed(1) : 0
    })).sort((a, b) => b.leads_sent - a.leads_sent);

    res.json({
      ok: true,
      as_of: new Date().toISOString(),
      window_weeks: weeks,
      scope: 'mva',
      ready_to_invoice: { total: +ready_total.toFixed(2), by_buyer: ready_by_buyer },
      awaiting_payment: {
        total: +awaiting_total.toFixed(2),
        overdue_total: +overdue_total.toFixed(2),
        oldest_overdue_days: oldest_overdue,
        by_invoice
      },
      paid_recent: {
        total: +paidWindowTotal.toFixed(2),
        by_buyer: Object.values(paidMap).map(p => ({ ...p, amount: +p.amount.toFixed(2) }))
      },
      by_buyer,
      payables: { total: +payables_total.toFixed(2), by_publisher: payables_by_publisher },
      net_position: +((ready_total + awaiting_total) - payables_total).toFixed(2),
      margins,
      conversion,
      float: { overall_days: floatOverall, cases: floatCases, by_publisher: float_by_publisher }
    });
  } catch (err) {
    console.error('[funding-summary]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── POST /billable-queue/invoice ──────────────────────
// Groups a buyer's approved-but-uninvoiced MVA cases under one invoice id.
// Marks our own rows only. Sends nothing to the buyer.
app.post('/billable-queue/invoice', requireKey, async (req, res) => {
  try {
    const body = req.body || {};
    const buyer = String(body.buyer || '').trim();
    if (!buyer) return res.status(400).json({ ok: false, error: 'buyer required' });

    let terms = parseInt(body.terms_days, 10);
    if (!isFinite(terms) || terms < 0 || terms > 180) terms = 3;

    const where = ["bq.status='approved'", 'bq.invoice_id IS NULL',
                   "COALESCE(NULLIF(TRIM(bq.raw->>'buyer'),''),'Unknown') = $1"];
    const params = [buyer];
    if (Array.isArray(body.ids) && body.ids.length) {
      const ids = body.ids.map(n => parseInt(n, 10)).filter(n => isFinite(n));
      if (!ids.length) return res.status(400).json({ ok: false, error: 'ids contained no valid numbers' });
      params.push(ids);
      where.push('bq.id = ANY($' + params.length + '::int[])');
    }

    const found = await pool.query(
      `SELECT bq.id, bq.amount::float AS amount, l.campaign
         FROM billable_queue bq
         LEFT JOIN leads l ON l.id = bq.lead_id
        WHERE ` + where.join(' AND '), params);

    // MVA only, same rule as the summary.
    const rows = found.rows.filter(r => fsIsMva(r.campaign));
    if (!rows.length) {
      return res.status(400).json({ ok: false, error: 'No approved, uninvoiced MVA cases for ' + buyer });
    }

    const slug = (buyer.replace(/[^A-Za-z0-9]+/g, '-').replace(/^-+|-+$/g, '').toUpperCase() || 'BUYER').slice(0, 12);
    const d = new Date();
    const ym = d.getFullYear() + String(d.getMonth() + 1).padStart(2, '0');
    const prefix = 'KRW-' + ym + '-' + slug + '-';
    const seqRow = await pool.query(
      `SELECT COUNT(DISTINCT invoice_id)::int AS n FROM billable_queue WHERE invoice_id LIKE $1`,
      [prefix + '%']);
    const invoice_id = prefix + String((seqRow.rows[0].n || 0) + 1).padStart(3, '0');

    const ids = rows.map(r => r.id);
    const upd = await pool.query(
      `UPDATE billable_queue
          SET invoice_id = $1, invoiced_at = NOW(), terms_days = $2,
              due_date = (CURRENT_DATE + ($2::int))
        WHERE id = ANY($3::int[])
        RETURNING id, due_date`,
      [invoice_id, terms, ids]);

    const amount = +rows.reduce((s, r) => s + fsNum(r.amount), 0).toFixed(2);
    const due_date = upd.rows[0] ? upd.rows[0].due_date : null;
    console.log('[Invoice] ' + invoice_id + ' | ' + buyer + ' | ' + upd.rows.length +
                ' case(s) | $' + amount + ' | due ' + due_date);
    return res.json({ ok: true, invoice_id, buyer, count: upd.rows.length, amount, due_date, terms_days: terms });
  } catch (err) {
    console.error('[billable-queue/invoice]', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

'''

src = src.replace(SCHEMA_ANCHOR, SCHEMA_ANCHOR + "\n" + SCHEMA_BLOCK, 1)
src = src.replace(ROUTE_ANCHOR, ROUTE_BLOCK + ROUTE_ANCHOR, 1)

shutil.copyfile(FILE, FILE + ".pre-336.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 336 applied")
print("  + billable_queue: invoice_id, invoiced_at, due_date, terms_days")
print("  + GET  /funding-summary")
print("  + POST /billable-queue/invoice")
print("backup at server.js.pre-336.bak")
print("commit and push to deploy")
