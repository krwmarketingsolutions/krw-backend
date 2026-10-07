#!/usr/bin/env python3
"""
patch-300.py  --  Let a specific call be billed at a specific amount

WHY
  Marking a call billable by hand currently has to go one of two ways, and
  neither does the job:

  1. PATCH /calls/:id/billable flips the boolean and nothing else. The call
     then reads "billable" with payout_amount 0.00, so it adds nothing to the
     publisher's payout - the portal and every revenue total read
     payout_amount, not the flag.

  2. The billable_queue approve path sets amount and label correctly, but it
     picks the call itself:
         ORDER BY (billable IS TRUE) DESC, received_at DESC LIMIT 1
     i.e. the MOST RECENT call for that number. When a caller shows up twice
     in a day (redials are common on these lines) that is often the wrong
     record - a 663-second redial instead of the 6,409-second call that
     actually converted.

  Kyler hit exactly this today: two Retained rows from Joshua's Signed line,
  one of the numbers having two calls, with the instruction "whichever one has
  longer duration push it through as billable".

WHAT IT CHANGES
  PATCH /calls/:id/billable now also accepts, both optional:
      payout_amount       a non-negative number, written as NUMERIC(10,2)
      call_status_label   e.g. 'cpa' (what every billed call on these lines
                          carries) or 'not_converted'
  and returns the updated row so the change can be verified without a second
  query.

  Strictly additive: each field is written ONLY if it is present in the body.
  The dashboard's existing toggle sends just {billable} and behaves exactly as
  it does today. A bad payout_amount is a 400 rather than a silent write, and
  an id that matches nothing is a 404 instead of a cheerful ok:true.

  Nothing about the billable_queue is touched. Its "most recent call" tiebreak
  is a separate question and is left alone deliberately.

Usage (from ~/krw/krw-backend):
    python3 patch-300.py
    node --check server.js
    git add -A && git commit -m "patch 300: bill a specific call at a specific amount" && git push

Backup: server.js.pre-300.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 300" in src:
    sys.exit("patch 300 already applied - nothing to do")

OLD = """app.patch('/calls/:id/billable', requireKey, async (req, res) => {
  try {
    const { billable } = req.body;
    await pool.query('UPDATE calls SET billable=$1 WHERE id=$2',[billable===true||billable==='true', req.params.id]);
    res.json({ ok:true });
  } catch(err) { res.status(500).json({ error:err.message }); }
});"""

if src.count(OLD) != 1:
    sys.exit("PATCH /calls/:id/billable does not look the way patch 300 expects - aborting, server.js untouched")

NEW = """app.patch('/calls/:id/billable', requireKey, async (req, res) => {
  try {
    const b = req.body || {};
    const billable = b.billable === true || b.billable === 'true';
    // patch 300: payout_amount and call_status_label are optional and are
    // written ONLY when explicitly sent, so the dashboard's existing toggle
    // (which sends just {billable}) behaves exactly as it did before.
    // Without them a call can be flagged billable while still carrying
    // payout_amount 0.00, which adds nothing to the publisher's payout.
    const sets = ['billable=$1'];
    const params = [billable];
    let i = 2;
    if (b.payout_amount !== undefined && b.payout_amount !== null && b.payout_amount !== '') {
      const amt = parseFloat(b.payout_amount);
      if (!isFinite(amt) || amt < 0) {
        return res.status(400).json({ ok: false, error: 'payout_amount must be a non-negative number' });
      }
      sets.push('payout_amount=$' + (i++));
      params.push(amt.toFixed(2));
    }
    if (b.call_status_label) {
      sets.push('call_status_label=$' + (i++));
      params.push(String(b.call_status_label));
    }
    params.push(req.params.id);
    const r = await pool.query(
      'UPDATE calls SET ' + sets.join(', ') + ' WHERE id=$' + i +
      ' RETURNING id, call_date, caller_id, call_duration, billable, payout_amount,' +
      ' call_status_label, disposition, publisher_sub, campaign',
      params);
    if (!r.rows[0]) return res.status(404).json({ ok: false, error: 'Call not found' });
    console.log(`[Calls] billable=${billable} | call ${r.rows[0].id} | CID ${r.rows[0].caller_id} | $${r.rows[0].payout_amount} | ${r.rows[0].call_duration}s`);
    res.json({ ok: true, call: r.rows[0] });
  } catch(err) { res.status(500).json({ error:err.message }); }
});"""

src = src.replace(OLD, NEW, 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-300.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 300 applied. backup: server.js.pre-300.bak")
print("  PATCH /calls/:id/billable now accepts payout_amount and call_status_label")
print("  and returns the updated row. Existing {billable}-only callers unchanged.")
print("")
print("next: node --check server.js && git add -A && git commit -m 'patch 300' && git push")
