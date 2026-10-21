#!/usr/bin/env python3
"""
patch-348.py  --  mark-paid: target an invoice or specific cases, and stop
                  silently skipping unapproved rows

TWO PROBLEMS WITH THE EXISTING ROUTE
/billable-queue/mark-paid (patch 334) filters on status='approved' and keys off
the buyer alone. Since patch 346 a pending or held case is owed money too, so
"clear this buyer" quietly skipped exactly the rows that had not been approved
and reported success anyway. Clearing MVA-003-LT today would have left Randy
Slater and Shakiah Harris behind without saying so.

And keying off the buyer is too blunt once invoices exist: one invoice being
settled is the normal event, not a whole buyer. Paying NLD's three invoiced
cases meant either sweeping all twelve of their cases or nothing.

WHAT CHANGES
  - Any owed row can be marked paid, whatever its approval state. Marking paid
    does not approve anything and still writes no lead, so a publisher portal
    is unaffected either way.
  - The route now accepts, in order of precedence:
        { ids: [..] }        exactly these cases
        { invoice_id: ".." } everything on that invoice
        { buyer: ".." }      that buyer, as before (+ optional from/to)
  - The response reports how many rows matched and the total, so "nothing
    matched" is visible rather than looking like success.

Backup: server.js.pre-348.bak    Safe to re-run: refuses twice.
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

if "patch 348" in src:
    sys.exit("patch 348 already applied - nothing to do")
if "patch 346" not in src:
    sys.exit("patch 346 is not applied - run patch-346.py first")

START = "// patch 334: mark a buyer's approved billables as paid (clears them from owed)"
END = "// patch 325: manually add a billable (hand-added signed cases, no postback)"
for name, a in (("mark-paid header", START), ("manual-add header", END)):
    if src.count(a) != 1:
        sys.exit("%s found %d times, expected 1 - aborting" % (name, src.count(a)))
a, b = src.index(START), src.index(END)
if b <= a:
    sys.exit("markers out of order - aborting, nothing changed")

NEW = r'''// patch 348: mark billables paid by id, by invoice, or by buyer.
// Since patch 346 a pending or held case is owed, so paying one must not
// require approval first - the old route filtered on status='approved' and
// silently skipped them. Marking paid writes no lead and approves nothing, so
// no publisher portal changes either way.
app.post('/billable-queue/mark-paid', requireKey, async (req, res) => {
  try {
    const b = req.body || {};
    const where = ["status <> 'paid'", "COALESCE(raw->>'voided','') <> 'true'"];
    const params = [];
    let i = 1, scope;

    if (Array.isArray(b.ids) && b.ids.length) {
      const ids = b.ids.map(n => parseInt(n, 10)).filter(n => isFinite(n));
      if (!ids.length) return res.status(400).json({ ok: false, error: 'ids contained no valid numbers' });
      params.push(ids); where.push('id = ANY($' + (i++) + '::int[])');
      scope = ids.length + ' case(s)';
    } else if (b.invoice_id) {
      params.push(String(b.invoice_id).trim()); where.push('invoice_id = $' + (i++));
      scope = 'invoice ' + b.invoice_id;
    } else if (b.buyer) {
      params.push(String(b.buyer).trim());
      where.push("COALESCE(NULLIF(TRIM(raw->>'buyer'),''),'') = $" + (i++));
      scope = 'buyer ' + b.buyer;
      if (b.from) { params.push(b.from); where.push('received_at >= $' + (i++)); }
      if (b.to)   { params.push(b.to);   where.push('received_at <= $' + (i++)); }
    } else {
      return res.status(400).json({ ok: false, error: 'pass ids, invoice_id, or buyer' });
    }

    const r = await pool.query(
      "UPDATE billable_queue SET status='paid', resolved_at=NOW(), " +
      "raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('paid_at', NOW()) " +
      'WHERE ' + where.join(' AND ') +
      " RETURNING id, amount, raw->>'buyer' AS buyer", params);

    const amount = +r.rows.reduce((s, x) => s + parseFloat(x.amount || 0), 0).toFixed(2);
    if (!r.rows.length) {
      return res.status(404).json({ ok: false, error: 'nothing owed matched ' + scope, count: 0, amount: 0 });
    }
    console.log('[Billable Paid] ' + scope + ' -> ' + r.rows.length + ' case(s), $' + amount);
    return res.json({ ok: true, scope, count: r.rows.length, amount, ids: r.rows.map(x => x.id) });
  } catch (err) { return res.status(500).json({ ok: false, error: err.message }); }
});

'''
src = src[:a] + NEW + src[b:]

shutil.copyfile(FILE, FILE + ".pre-348.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 348 applied - mark-paid by ids / invoice_id / buyer, no longer skips unapproved")
print("backup at server.js.pre-348.bak")
