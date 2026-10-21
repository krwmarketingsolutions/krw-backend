#!/usr/bin/env python3
"""
patch-346.py  --  billable means owed; plus amount edits and voiding

RULE CHANGE (Kyler, 2026-10-20)
"When a lead is marked billable by any buyer sheet, it automatically comes to
the invoice tab. For me to update the pub portal, it must be approved by me."

So approval is about the PUBLISHER PORTAL, not about whether a buyer owes the
money. Up to now pending and held were pushed into `unconfirmed` and kept out
of owed, out of P&L and out of margin. That understated what buyers owe by
whatever was sitting unapproved - $9,400 at the time of writing.

From here every non-paid row with a buyer counts as owed, whatever its approval
status, and pending/held rows are included in P&L and margin too, since the
revenue is earned and the publisher cost is incurred regardless of when the
portal gets updated.

What approval still controls is unchanged and untouched: the existing approve
route is what writes the lead and surfaces the payout on the publisher portal.
This patch does not approve anything or write any lead.

`awaiting_approval` is reported separately so the dashboard can still show what
has not been approved for the publisher yet - it is now a note about portals,
not a deduction from what is owed.

ALSO ADDS
  POST /billable-queue/:id/amount  {amount}  - correct what a buyer pays on a
    case (NLD moving from $2,000 to $2,250 per CPA has no other route in).
  POST /billable-queue/:id/void    {reason?} - take a row off the invoice sheet
    without deleting history. Voided rows are excluded from every figure.

Voiding is the safe way to resolve a duplicate: the row stays in the table with
raw.voided and a reason, so the correction is auditable rather than silent.

Backup: server.js.pre-346.bak    Safe to re-run: refuses twice.
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

if "patch 346" in src:
    sys.exit("patch 346 already applied - nothing to do")
if "patch 345" not in src:
    sys.exit("patch 345 is not applied - run patch-345.py first")

# ── 1. Exclude voided rows from the summary ──────────────────────────────
OLD_WHERE = "        WHERE bq.status IN ('approved','paid','pending','held')"
NEW_WHERE = ("        WHERE bq.status IN ('approved','paid','pending','held')\n"
             "          AND COALESCE(bq.raw->>'voided','') <> 'true'")
if src.count(OLD_WHERE) != 1:
    sys.exit("summary WHERE found %d times, expected 1 - aborting" % src.count(OLD_WHERE))
src = src.replace(OLD_WHERE, NEW_WHERE, 1)

# ── 2. Pending/held now count as owed instead of being sidelined ─────────
OLD_SKIP = """      /* ── Not yours yet: reported by a buyer, not approved by you ── */
      if (r.status === 'pending' || r.status === 'held') {
        unconf[r.status].total += amt;
        unconf[r.status].count++;
        const u = unconfBuyer[r.buyer] || (unconfBuyer[r.buyer] =
          { buyer: r.buyer, pending: 0, held: 0, count: 0 });
        u[r.status] += amt; u.count++;
        return;   // excluded from payables, margin, float and P&L
      }
"""
NEW_SKIP = """      /* patch 346: a buyer sheet marking a case billable is what makes it owed.
         Approval is about updating the publisher portal, so pending and held
         are counted here and only noted separately for the portal's sake. */
      if (r.status === 'pending' || r.status === 'held') {
        unconf[r.status].total += amt;
        unconf[r.status].count++;
        const u = unconfBuyer[r.buyer] || (unconfBuyer[r.buyer] =
          { buyer: r.buyer, pending: 0, held: 0, count: 0 });
        u[r.status] += amt; u.count++;
      }
      const fsOwed = r.status !== 'paid';
"""
if src.count(OLD_SKIP) != 1:
    sys.exit("pending/held block found %d times, expected 1 - aborting" % src.count(OLD_SKIP))
src = src.replace(OLD_SKIP, NEW_SKIP, 1)

# Every "approved" test in the money sections becomes "owed".
for old, new, label in (
    ("      if (r.status === 'approved' && !r.invoiced_at) {",
     "      if (fsOwed && !r.invoiced_at) {", "ready"),
    ("      if (r.status === 'approved' && r.invoiced_at) {",
     "      if (fsOwed && r.invoiced_at) {", "invoiced"),
    ("      if (r.status === 'approved') {\n        const p = payMap[pub]",
     "      if (fsOwed) {\n        const p = payMap[pub]", "payables/detail"),
):
    if src.count(old) != 1:
        sys.exit("%s branch found %d times, expected 1 - aborting" % (label, src.count(old)))
    src = src.replace(old, new, 1)

# Detail rows carry their approval state so the UI can mark them.
OLD_DETAIL = "          source: r.invoice_only ? 'manual' : 'sheet'"
NEW_DETAIL = ("          source: r.invoice_only ? 'manual' : 'sheet',\n"
              "          approval: r.status,\n"
              "          awaiting_approval: (r.status === 'pending' || r.status === 'held')")
if src.count(OLD_DETAIL) != 1:
    sys.exit("detail tail found %d times, expected 1 - aborting" % src.count(OLD_DETAIL))
src = src.replace(OLD_DETAIL, NEW_DETAIL, 1)

# ── 3. Rename the reported block: a portal note, not a deduction ─────────
OLD_UNCONF = """      unconfirmed: {
        total: +(unconf.pending.total + unconf.held.total).toFixed(2),"""
NEW_UNCONF = """      awaiting_approval: {
        note: 'Counted in what the buyer owes. Not yet approved, so the payout is not on the publisher portal.',
        total: +(unconf.pending.total + unconf.held.total).toFixed(2),"""
if src.count(OLD_UNCONF) != 1:
    sys.exit("unconfirmed block found %d times, expected 1 - aborting" % src.count(OLD_UNCONF))
src = src.replace(OLD_UNCONF, NEW_UNCONF, 1)
# keep the old key too so an un-updated dashboard keeps rendering
OLD_TAIL = """      paid_recent: {"""
NEW_TAIL = """      unconfirmed: {
        total: +(unconf.pending.total + unconf.held.total).toFixed(2),
        pending: { total: +unconf.pending.total.toFixed(2), count: unconf.pending.count },
        held: { total: +unconf.held.total.toFixed(2), count: unconf.held.count },
        by_buyer: Object.values(unconfBuyer).map(u => ({
          buyer: u.buyer, pending: +u.pending.toFixed(2), held: +u.held.toFixed(2), count: u.count
        })).sort((a, b) => (b.pending + b.held) - (a.pending + a.held))
      },
      paid_recent: {"""
if src.count(OLD_TAIL) != 1:
    sys.exit("paid_recent anchor found %d times, expected 1 - aborting" % src.count(OLD_TAIL))
src = src.replace(OLD_TAIL, NEW_TAIL, 1)

# ── 4. Amount correction and voiding ─────────────────────────────────────
ROUTE_ANCHOR = "app.get('/billable-queue', requireKey, async (req, res) => {"
if src.count(ROUTE_ANCHOR) != 1:
    sys.exit("route anchor found %d times, expected 1 - aborting" % src.count(ROUTE_ANCHOR))

ROUTES = r'''// ══════════════════════════════════════════════════════
//  patch 346: correct an amount, or take a row off the sheet
// ══════════════════════════════════════════════════════

/* What a buyer pays on one case. Used when a buyer's CPA changes and existing
   rows need to catch up. Does not touch the lead or the publisher portal. */
app.post('/billable-queue/:id/amount', requireKey, async (req, res) => {
  try {
    const amt = Number((req.body || {}).amount);
    if (!(amt > 0)) return res.status(400).json({ ok: false, error: 'amount must be a positive number' });
    const before = await pool.query('SELECT amount FROM billable_queue WHERE id=$1', [req.params.id]);
    if (!before.rows[0]) return res.status(404).json({ ok: false, error: 'not found' });
    const old = parseFloat(before.rows[0].amount || 0);
    const r = await pool.query(
      `UPDATE billable_queue
          SET amount = $1::numeric,
              raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object(
                'amount_history', COALESCE(raw->'amount_history','[]'::jsonb) ||
                  jsonb_build_array(jsonb_build_object('from', $2::text, 'to', $1::text, 'at', NOW()))
              )
        WHERE id = $3 RETURNING id, cid, amount, raw->>'buyer' AS buyer`,
      [amt.toFixed(2), old.toFixed(2), req.params.id]);
    console.log('[Billable Amount] q' + req.params.id + ' $' + old + ' -> $' + amt);
    return res.json({ ok: true, item: r.rows[0], previous: old });
  } catch (err) { return res.status(500).json({ ok: false, error: err.message }); }
});

/* Take a row off the invoice sheet without deleting it. The row and its reason
   stay in the table, so a correction can be looked at later. */
app.post('/billable-queue/:id/void', requireKey, async (req, res) => {
  try {
    const reason = String((req.body || {}).reason || '').trim() || 'voided';
    const r = await pool.query(
      `UPDATE billable_queue
          SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object(
                'voided', true, 'void_reason', $1::text, 'voided_at', NOW())
        WHERE id = $2 RETURNING id, cid, amount, raw->>'buyer' AS buyer`,
      [reason, req.params.id]);
    if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'not found' });
    console.log('[Billable Void] q' + req.params.id + ' (' + reason + ')');
    return res.json({ ok: true, item: r.rows[0] });
  } catch (err) { return res.status(500).json({ ok: false, error: err.message }); }
});

/* Undo a void. */
app.post('/billable-queue/:id/unvoid', requireKey, async (req, res) => {
  try {
    const r = await pool.query(
      `UPDATE billable_queue SET raw = (COALESCE(raw,'{}'::jsonb) - 'voided' - 'void_reason' - 'voided_at')
        WHERE id = $1 RETURNING id, cid, amount`, [req.params.id]);
    if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'not found' });
    return res.json({ ok: true, item: r.rows[0] });
  } catch (err) { return res.status(500).json({ ok: false, error: err.message }); }
});

'''
src = src.replace(ROUTE_ANCHOR, ROUTES + ROUTE_ANCHOR, 1)

shutil.copyfile(FILE, FILE + ".pre-346.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 346 applied")
print("  + pending/held now count as owed; approval governs the portal only")
print("  + POST /billable-queue/:id/amount, /void, /unvoid")
print("  + voided rows excluded from every figure")
print("backup at server.js.pre-346.bak")
