#!/usr/bin/env python3
"""
patch-343.py  --  case counts per buyer, and a date range filter

TWO ADDITIONS

1. CASE COUNTS
   by_buyer carried only ready_count, so the dashboard could show what each
   buyer owes but not how many cases that is. Adds invoiced_count,
   overdue_count, unconfirmed_count and cases (ready + invoiced, i.e. the
   count behind the owed figure).

2. PER-LEAD DETAIL
   by_buyer now carries `detail`: every case behind that buyer's owed figure,
   with the lead's name, phone, state, publisher, what the buyer pays, what the
   publisher is paid and the margin on it, whether it is invoiced and how old
   it is. That is what the dashboard expands when a buyer is clicked.

3. RANGE FILTER  ?range=week|biweek|month|all
   Rolling windows of 7, 14 and 30 days measured from the signed date, which
   is when the case was earned. Applies to every money section and to the P&L
   so the whole view moves together rather than mixing periods.

   DEFAULT IS `all`, deliberately. A filtered default would quietly hide older
   uninvoiced cases, and money that scrolls out of view is money that does not
   get chased. Narrowing is something you choose, never something that happens
   to you.

   `weeks` still works for conversion and is derived from range when range is
   given, so the two cannot disagree.

Backup: server.js.pre-343.bak    Safe to re-run: refuses twice.
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

if "patch 343" in src:
    sys.exit("patch 343 already applied - nothing to do")
if "patch 342" not in src:
    sys.exit("patch 342 is not applied - run patch-342.py first")

# ── 1. Parse the range and derive the window ──────────────────────────────
OLD_HEAD = """    const weeks = Math.max(1, Math.min(52, parseInt(req.query.weeks || '8', 10) || 8));
    const today = fsToday();"""
NEW_HEAD = """    /* patch 343: rolling window on the signed date. Default 'all' on purpose -
       a filtered default would hide older uninvoiced cases, and money out of
       sight is money that does not get chased. */
    const RANGE_DAYS = { week: 7, biweek: 14, month: 30 };
    const range = ['week', 'biweek', 'month', 'all'].indexOf(String(req.query.range || 'all')) > -1
      ? String(req.query.range || 'all') : 'all';
    const rangeDays = RANGE_DAYS[range] || null;
    const weeks = rangeDays
      ? Math.max(1, Math.round(rangeDays / 7))
      : Math.max(1, Math.min(52, parseInt(req.query.weeks || '8', 10) || 8));
    const today = fsToday();
    const rangeSince = rangeDays ? new Date(today.getTime() - rangeDays * 86400000) : null;"""
if src.count(OLD_HEAD) != 1:
    sys.exit("header block found %d times, expected 1 - aborting" % src.count(OLD_HEAD))
src = src.replace(OLD_HEAD, NEW_HEAD, 1)

# ── 2. Apply the window to the rows the whole summary is built from ───────
OLD_FILTER = "    const rows = all.rows.filter(fsRowIsMva);"
NEW_FILTER = """    const rows = all.rows.filter(fsRowIsMva).filter(r =>
      !rangeSince || (r.signed_at && new Date(r.signed_at) >= rangeSince));"""
if src.count(OLD_FILTER) != 1:
    sys.exit("row filter found %d times, expected 1 - aborting" % src.count(OLD_FILTER))
src = src.replace(OLD_FILTER, NEW_FILTER, 1)

# ── 3. Count cases alongside the amounts ──────────────────────────────────
OLD_TOUCH = """    const touch = (name) => buyers[name] || (buyers[name] = {
      buyer: name, ready: 0, ready_count: 0, invoiced_unpaid: 0,
      overdue: 0, paid_window: 0, pending: 0, held: 0, total_owed: 0
    });"""
NEW_TOUCH = """    const touch = (name) => buyers[name] || (buyers[name] = {
      buyer: name, ready: 0, ready_count: 0, invoiced_unpaid: 0, invoiced_count: 0,
      overdue: 0, overdue_count: 0, paid_window: 0, paid_count: 0,
      pending: 0, held: 0, unconfirmed_count: 0, total_owed: 0
    });"""
if src.count(OLD_TOUCH) != 1:
    sys.exit("buyer accumulator found %d times, expected 1 - aborting" % src.count(OLD_TOUCH))
src = src.replace(OLD_TOUCH, NEW_TOUCH, 1)

OLD_INV = """    by_invoice.forEach(v => {
      const x = touch(v.buyer);
      x.invoiced_unpaid += v.amount;
      if (v.overdue) x.overdue += v.amount;
    });"""
NEW_INV = """    by_invoice.forEach(v => {
      const x = touch(v.buyer);
      x.invoiced_unpaid += v.amount;
      x.invoiced_count += v.count;
      if (v.overdue) { x.overdue += v.amount; x.overdue_count += v.count; }
    });"""
if src.count(OLD_INV) != 1:
    sys.exit("invoice rollup found %d times, expected 1 - aborting" % src.count(OLD_INV))
src = src.replace(OLD_INV, NEW_INV, 1)

OLD_PAID = """    Object.values(paidMap).forEach(p => { touch(p.buyer).paid_window = +p.amount.toFixed(2); });"""
NEW_PAID = """    Object.values(paidMap).forEach(p => {
      const x = touch(p.buyer); x.paid_window = +p.amount.toFixed(2); x.paid_count = p.count;
    });"""
if src.count(OLD_PAID) != 1:
    sys.exit("paid rollup found %d times, expected 1 - aborting" % src.count(OLD_PAID))
src = src.replace(OLD_PAID, NEW_PAID, 1)

OLD_UNC = """    Object.values(unconfBuyer).forEach(u => {
      const x = touch(u.buyer);
      x.pending = +u.pending.toFixed(2);
      x.held = +u.held.toFixed(2);
    });"""
NEW_UNC = """    Object.values(unconfBuyer).forEach(u => {
      const x = touch(u.buyer);
      x.pending = +u.pending.toFixed(2);
      x.held = +u.held.toFixed(2);
      x.unconfirmed_count = u.count;
    });"""
if src.count(OLD_UNC) != 1:
    sys.exit("unconfirmed rollup found %d times, expected 1 - aborting" % src.count(OLD_UNC))
src = src.replace(OLD_UNC, NEW_UNC, 1)

OLD_MAP = """      total_owed: +(x.ready + x.invoiced_unpaid).toFixed(2),
      unconfirmed: +(x.pending + x.held).toFixed(2)"""
NEW_MAP = """      total_owed: +(x.ready + x.invoiced_unpaid).toFixed(2),
      cases: x.ready_count + x.invoiced_count,
      unconfirmed: +(x.pending + x.held).toFixed(2)"""
if src.count(OLD_MAP) != 1:
    sys.exit("buyer map found %d times, expected 1 - aborting" % src.count(OLD_MAP))
src = src.replace(OLD_MAP, NEW_MAP, 1)

# ── 4. Report the active range back so the UI can reflect it ──────────────
OLD_RESP = """      window_weeks: weeks,
      scope: 'mva',"""
NEW_RESP = """      window_weeks: weeks,
      range: range,
      range_days: rangeDays,
      scope: 'mva',"""
if src.count(OLD_RESP) != 1:
    sys.exit("response head found %d times, expected 1 - aborting" % src.count(OLD_RESP))
src = src.replace(OLD_RESP, NEW_RESP, 1)


# ── 3b. Per-lead breakdown behind each buyer's owed figure ────────────────
OLD_ACC = "    const unconf = { pending: { total: 0, count: 0 }, held: { total: 0, count: 0 } };"
if src.count(OLD_ACC) != 1:
    sys.exit("unconf accumulator found %d times, expected 1 - aborting" % src.count(OLD_ACC))
src = src.replace(OLD_ACC, OLD_ACC + "\n    const ownedDetail = {};", 1)

OLD_PAY = """      if (r.status === 'approved') {
        const p = payMap[pub] || (payMap[pub] = { publisher: pub, count: 0, amount: 0 });
        p.count++; p.amount += rate;
      }"""
NEW_PAY = """      if (r.status === 'approved') {
        const p = payMap[pub] || (payMap[pub] = { publisher: pub, count: 0, amount: 0 });
        p.count++; p.amount += rate;

        /* patch 343: the lead-level breakdown behind this buyer's owed figure. */
        const dAge = fsDays(r.signed_at, today);
        let dOver = false, dOverDays = 0;
        if (r.invoiced_at && r.due_date) {
          const o = fsDays(new Date(r.due_date), today);
          dOverDays = o !== null && o > 0 ? Math.floor(o) : 0;
          dOver = dOverDays > 0;
        }
        (ownedDetail[r.buyer] || (ownedDetail[r.buyer] = [])).push({
          id: r.id, cid: r.cid,
          name: r.manual_name || [r.first_name, r.last_name].filter(Boolean).join(' ') || null,
          state: r.state || null, publisher_sub: pub,
          amount: +amt.toFixed(2), payout: +rate.toFixed(2), margin: +(amt - rate).toFixed(2),
          cost_known: fsCostKnown,
          signed_at: r.signed_at, days_since_signed: dAge === null ? null : Math.floor(dAge),
          invoiced: !!r.invoiced_at, invoice_id: r.invoice_id || null,
          due_date: r.due_date || null, overdue: dOver, days_overdue: dOverDays,
          source: r.invoice_only ? 'manual' : 'sheet'
        });
      }"""
if src.count(OLD_PAY) != 1:
    sys.exit("payables block found %d times, expected 1 - aborting" % src.count(OLD_PAY))
src = src.replace(OLD_PAY, NEW_PAY, 1)

OLD_DET = "      cases: x.ready_count + x.invoiced_count,"
NEW_DET = ("      cases: x.ready_count + x.invoiced_count,\n"
           "      detail: (ownedDetail[x.buyer] || []).slice().sort((a, b) =>\n"
           "        (b.days_since_signed || 0) - (a.days_since_signed || 0)),")
if src.count(OLD_DET) != 1:
    sys.exit("buyer cases line found %d times, expected 1 - aborting" % src.count(OLD_DET))
src = src.replace(OLD_DET, NEW_DET, 1)

shutil.copyfile(FILE, FILE + ".pre-343.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 343 applied")
print("  + case counts per buyer (ready, invoiced, overdue, unconfirmed, total)")
print("  + per-lead detail behind each buyer")
print("  + ?range=week|biweek|month|all  (default all)")
print("backup at server.js.pre-343.bak")
