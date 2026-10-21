#!/usr/bin/env python3
"""
patch-338.py  --  held/pending money + weekly and monthly P&L

Replaces the /funding-summary handler added in patch 336 with a version that:

  1. COUNTS HELD AND PENDING. Patch 336 only looked at approved and paid, so a
     case a buyer reported and you parked (held) or have not approved yet
     (pending) was invisible. Both now come back under `unconfirmed`, kept
     separate from money you have actually approved, because neither is owed to
     you until you say so.

  2. ADDS A P&L. Revenue, publisher cost and profit by week and by month, plus
     the same split per buyer and per publisher.

WHAT "PROFIT" MEANS HERE
  Revenue (what the buyer pays) minus publisher payout, on cases signed in that
  period. That is GROSS MARGIN ON LEAD SALES. It does not include ad spend,
  which is not tracked yet, nor payroll, software or any other overhead. Treat
  it as "what each lane earned before the cost of getting the leads", not as
  net profit. Once Meta and Google spend lands in the books this is where it
  gets subtracted.

  Revenue is recognised on the SIGNED date, not when the buyer pays, so a week
  shows what it earned rather than what happened to land in the bank. Held and
  pending cases are excluded from P&L entirely, since they are not yours yet.

MVA only, same campaign rules as before. Still read-only. The invoice route
from patch 336 is untouched.

Backup: server.js.pre-338.bak    Safe to re-run: refuses twice.
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

if "patch 338" in src:
    sys.exit("patch 338 already applied - nothing to do")
if "patch 336" not in src:
    sys.exit("patch 336 is not applied - run patch-336.py first")

START = "// ══ GET /funding-summary ══"
OPEN = "app.get('/funding-summary', requireKey, async (req, res) => {"
END = "// ── POST /billable-queue/invoice"

# Patch 336 wrote the handler with a plain comment header; locate by the route.
if src.count(OPEN) != 1:
    sys.exit("expected exactly 1 /funding-summary handler, found %d - aborting" % src.count(OPEN))
if src.count(END) != 1:
    sys.exit("could not locate the invoice route marker - aborting, nothing changed")

open_at = src.index(OPEN)
end_at = src.index(END)
if end_at < open_at:
    sys.exit("file layout unexpected - aborting, nothing changed")

NEW = r'''app.get('/funding-summary', requireKey, async (req, res) => {
  try {
    const weeks = Math.max(1, Math.min(52, parseInt(req.query.weeks || '8', 10) || 8));
    const today = fsToday();

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

    /* patch 338: pending and held are read too, so money a buyer has reported
       but you have not approved is visible instead of silently missing. */
    const all = await pool.query(
      `SELECT bq.id, bq.cid, bq.status, bq.amount::float AS amount, bq.publisher_sub,
              bq.received_at AS signed_at, bq.invoice_id, bq.invoiced_at,
              bq.due_date, bq.terms_days, bq.resolved_at,
              COALESCE(NULLIF(TRIM(bq.raw->>'buyer'),''),'Unknown') AS buyer,
              l.campaign, l.state, l.first_name, l.last_name,
              l.received_at AS lead_at
         FROM billable_queue bq
         LEFT JOIN leads l ON l.id = bq.lead_id
        WHERE bq.status IN ('approved','paid','pending','held')
        ORDER BY bq.received_at DESC`);

    const rows = all.rows.filter(r => fsIsMva(r.campaign));

    const readyMap = {}, invMap = {}, paidMap = {}, payMap = {}, laneMap = {}, floatMap = {};
    const unconf = { pending: { total: 0, count: 0 }, held: { total: 0, count: 0 } };
    const unconfBuyer = {};
    const wkMap = {}, moMap = {}, pnlBuyer = {}, pnlPub = {};
    let paidWindowTotal = 0;
    const paidSince = new Date(today.getTime() - weeks * 7 * 86400000);

    const weekStart = (d) => {
      const x = new Date(d); x.setHours(0, 0, 0, 0);
      const dow = (x.getDay() + 6) % 7;           // Monday = 0
      x.setDate(x.getDate() - dow);
      return x.toISOString().slice(0, 10);
    };
    const monthStart = (d) => new Date(d).toISOString().slice(0, 7);

    rows.forEach(r => {
      const pub = r.publisher_sub || 'Unknown';
      const rate = rateFor(r.publisher_sub, r.campaign);
      const amt = fsNum(r.amount);

      /* ── Not yours yet: reported by a buyer, not approved by you ── */
      if (r.status === 'pending' || r.status === 'held') {
        unconf[r.status].total += amt;
        unconf[r.status].count++;
        const u = unconfBuyer[r.buyer] || (unconfBuyer[r.buyer] =
          { buyer: r.buyer, pending: 0, held: 0, count: 0 });
        u[r.status] += amt; u.count++;
        return;   // excluded from payables, margin, float and P&L
      }

      if (r.status === 'approved' && !r.invoiced_at) {
        const b = readyMap[r.buyer] || (readyMap[r.buyer] =
          { buyer: r.buyer, count: 0, amount: 0, oldest_days: 0, cases: [] });
        const age = fsDays(r.signed_at, today);
        b.count++; b.amount += amt;
        if (age !== null && age > b.oldest_days) b.oldest_days = Math.floor(age);
        b.cases.push({
          id: r.id, cid: r.cid, amount: amt, publisher_sub: pub,
          signed_at: r.signed_at, first_name: r.first_name, last_name: r.last_name,
          state: r.state, days_since_signed: age === null ? null : Math.floor(age)
        });
      }

      if (r.status === 'approved' && r.invoiced_at) {
        const key = r.invoice_id || ('no-id-' + r.id);
        const v = invMap[key] || (invMap[key] = {
          invoice_id: r.invoice_id, buyer: r.buyer, count: 0, amount: 0,
          invoiced_at: r.invoiced_at, due_date: r.due_date,
          days_outstanding: 0, days_overdue: 0, overdue: false
        });
        v.count++; v.amount += amt;
        const out = fsDays(r.invoiced_at, today);
        v.days_outstanding = out === null ? 0 : Math.floor(out);
        if (r.due_date) {
          const over = fsDays(new Date(r.due_date), today);
          v.days_overdue = over !== null && over > 0 ? Math.floor(over) : 0;
          v.overdue = v.days_overdue > 0;
        }
      }

      if (r.status === 'paid') {
        const when = r.resolved_at || r.signed_at;
        if (when && new Date(when) >= paidSince) {
          const p = paidMap[r.buyer] || (paidMap[r.buyer] = { buyer: r.buyer, count: 0, amount: 0 });
          p.count++; p.amount += amt; paidWindowTotal += amt;
        }
      }

      if (r.status === 'approved') {
        const p = payMap[pub] || (payMap[pub] = { publisher: pub, count: 0, amount: 0 });
        p.count++; p.amount += rate;
      }

      const lk = pub + ' → ' + r.buyer;
      const L = laneMap[lk] || (laneMap[lk] = {
        lane: lk, publisher: pub, buyer: r.buyer, vertical: fsVertical(r.campaign),
        count: 0, buyer_pays: 0, publisher_gets: 0
      });
      L.count++; L.buyer_pays += amt; L.publisher_gets += rate;

      const f = fsDays(r.lead_at, r.signed_at);
      if (f !== null && f >= 0 && f <= 365) {
        const fm = floatMap[pub] || (floatMap[pub] = { publisher: pub, total: 0, count: 0 });
        fm.total += f; fm.count++;
      }

      /* ── P&L: earned on the signed date, approved and paid only ── */
      if (r.signed_at) {
        const bump = (m, k) => {
          const e = m[k] || (m[k] = { key: k, revenue: 0, cost: 0, profit: 0, cases: 0 });
          e.revenue += amt; e.cost += rate; e.profit += (amt - rate); e.cases++;
        };
        bump(wkMap, weekStart(r.signed_at));
        bump(moMap, monthStart(r.signed_at));
        bump(pnlBuyer, r.buyer);
        bump(pnlPub, pub);
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

    const buyers = {};
    const touch = (name) => buyers[name] || (buyers[name] = {
      buyer: name, ready: 0, ready_count: 0, invoiced_unpaid: 0,
      overdue: 0, paid_window: 0, pending: 0, held: 0, total_owed: 0
    });
    ready_by_buyer.forEach(b => { const x = touch(b.buyer); x.ready = b.amount; x.ready_count = b.count; });
    by_invoice.forEach(v => {
      const x = touch(v.buyer);
      x.invoiced_unpaid += v.amount;
      if (v.overdue) x.overdue += v.amount;
    });
    Object.values(paidMap).forEach(p => { touch(p.buyer).paid_window = +p.amount.toFixed(2); });
    Object.values(unconfBuyer).forEach(u => {
      const x = touch(u.buyer);
      x.pending = +u.pending.toFixed(2);
      x.held = +u.held.toFixed(2);
    });
    const by_buyer = Object.values(buyers).map(x => ({
      ...x,
      invoiced_unpaid: +x.invoiced_unpaid.toFixed(2),
      overdue: +x.overdue.toFixed(2),
      total_owed: +(x.ready + x.invoiced_unpaid).toFixed(2),
      unconfirmed: +(x.pending + x.held).toFixed(2)
    })).sort((a, b) => (b.total_owed - a.total_owed) || (b.unconfirmed - a.unconfirmed));

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
      c.leads_sent += r.leads_sent; c.signed += r.signed;
    });
    const conversion = Object.values(convMap).map(c => ({
      ...c, rate: c.leads_sent ? +((c.signed / c.leads_sent) * 100).toFixed(1) : 0
    })).sort((a, b) => b.leads_sent - a.leads_sent);

    const tidy = (m, limit) => Object.values(m)
      .map(e => ({
        key: e.key, cases: e.cases,
        revenue: +e.revenue.toFixed(2), cost: +e.cost.toFixed(2), profit: +e.profit.toFixed(2),
        margin_pct: e.revenue ? +((e.profit / e.revenue) * 100).toFixed(1) : 0
      }))
      .sort((a, b) => (a.key < b.key ? 1 : -1))
      .slice(0, limit);

    const pnlTotals = Object.values(pnlBuyer).reduce(
      (t, e) => ({ revenue: t.revenue + e.revenue, cost: t.cost + e.cost, profit: t.profit + e.profit, cases: t.cases + e.cases }),
      { revenue: 0, cost: 0, profit: 0, cases: 0 });

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
      unconfirmed: {
        total: +(unconf.pending.total + unconf.held.total).toFixed(2),
        pending: { total: +unconf.pending.total.toFixed(2), count: unconf.pending.count },
        held: { total: +unconf.held.total.toFixed(2), count: unconf.held.count },
        by_buyer: Object.values(unconfBuyer).map(u => ({
          buyer: u.buyer, pending: +u.pending.toFixed(2), held: +u.held.toFixed(2), count: u.count
        })).sort((a, b) => (b.pending + b.held) - (a.pending + a.held))
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
      float: { overall_days: floatOverall, cases: floatCases, by_publisher: float_by_publisher },
      pnl: {
        basis: 'Gross margin on signed cases: buyer revenue minus publisher payout. Excludes ad spend and overhead.',
        totals: {
          revenue: +pnlTotals.revenue.toFixed(2), cost: +pnlTotals.cost.toFixed(2),
          profit: +pnlTotals.profit.toFixed(2), cases: pnlTotals.cases,
          margin_pct: pnlTotals.revenue ? +((pnlTotals.profit / pnlTotals.revenue) * 100).toFixed(1) : 0
        },
        weekly: tidy(wkMap, 12),
        monthly: tidy(moMap, 6),
        by_buyer: tidy(pnlBuyer, 20).sort((a, b) => b.profit - a.profit),
        by_publisher: tidy(pnlPub, 20).sort((a, b) => b.profit - a.profit)
      }
    });
  } catch (err) {
    console.error('[funding-summary]', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

'''

src = src[:open_at] + NEW + src[end_at:]
src = src.replace(OPEN, "// patch 338: held/pending + P&L\n" + OPEN, 1)

shutil.copyfile(FILE, FILE + ".pre-338.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 338 applied")
print("  + unconfirmed: pending and held money, per buyer")
print("  + pnl: weekly, monthly, by buyer, by publisher")
print("backup at server.js.pre-338.bak")
