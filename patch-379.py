#!/usr/bin/env python3
r"""
patch-379.py  --  new MVA routing: NLD-first-or-hold, then a price-point bid

Kyler, Nov 10:
  - NLD always gets the lead first on its own states.
  - On an NLD state, if NLD does not take the lead (reject / error / over cap /
    missing fields), HOLD it for review instead of auto-selling to a cheaper
    buyer. Nothing on an NLD state reaches the bottom tier automatically.
  - On every other state the bottom tier (CAG / LAG / Eilers) competes by
    price point: highest payout first, fall to the next on a reject.
  - Resend / push-through runs NLD first (its states) then the same price bid,
    so a held lead can be pushed to an endpoint manually. DT-Intake / CH-Intake
    are retired from the resend plan.

Edits (krw-backend/server.js):
  1. mva-nyc-split: replace the eligible/sort block with the NLD-gate + bid.
  2. empty-eligible hold message is NLD-aware.
  3. final write: NLD-state non-accept -> status 'received' (Held), buyer_error
     set, buyer_name cleared (no buyer actually took it).
  4. response result -> 'held' on an NLD-state hold.
  5. janSend: add CAG + Legal Advocate Group senders (for resend).
  6. resend-ladder plan: NLD first (its states) then CAG/LAG/Eilers by payout.

Backup: server.js.pre-379.bak    Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None
ROOT = find_root() or sys.exit("run from ~/krw (needs krw-backend/)")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()
if "patch 379" in src:
    sys.exit("patch 379 already applied - nothing to do")

edits = [
# 1. eligible/sort -> NLD gate or price bid
("""  const eligible = LADDER_FOR_PUB.filter(buyer => {   // patch 299
    if (!buyer.enabled) return false;
    if (buyer.states !== 'ALL' && !buyer.states.includes(leadState)) return false;
    if (buyer.cap != null && (todayCount[buyer.name] || 0) >= buyer.cap) return false;
    if (buyer.name === 'NLD CPA' && nldMissing.length) return false;
    return true;
  }).sort((a, b2) => {
    if (a.priority !== b2.priority) return a.priority - b2.priority;
    // same rung: the buyer with fewer accepted today goes first; on a tie,
    // whoever did NOT get the most recent one - this is the 50/50.
    // patch 317: Leadbloom balances its own sends, not the campaign's.
    const tc = PUB === 'KRW-LEADBLOOM-MVA' ? lbCount : todayCount;
    const la = PUB === 'KRW-LEADBLOOM-MVA' ? lbLast : lastAt;
    const ca = tc[a.name] || 0, cb = tc[b2.name] || 0;
    if (ca !== cb) return ca - cb;
    return (la[a.name] || 0) - (la[b2.name] || 0);
  });""",
"""  // patch 379: NLD is the first-priority gate on its own states. On an NLD
  // state NLD alone is offered; if NLD does not take it the lead HOLDS for
  // Kyler's review (never auto-sells to the bottom tier). On every other state
  // the bottom tier (CAG / LAG / Eilers) competes by price point - highest
  // payout first, first to accept wins.
  const isNldState = NLD_ONLY_STATES.includes(leadState) && !NLD_PAUSED;
  let eligible;
  if (isNldState) {
    eligible = LADDER_FOR_PUB.filter(x =>
      x.name === 'NLD CPA' && x.enabled &&
      (x.cap == null || (todayCount[x.name] || 0) < x.cap) &&
      !nldMissing.length);
  } else {
    eligible = LADDER_FOR_PUB.filter(buyer => {
      if (!buyer.enabled) return false;
      if (buyer.name === 'NLD CPA') return false;   // NLD only serves its own states
      if (buyer.states !== 'ALL' && !buyer.states.includes(leadState)) return false;
      if (buyer.cap != null && (todayCount[buyer.name] || 0) >= buyer.cap) return false;
      return true;
    }).sort((a, b2) => (Number(b2.payout) || 0) - (Number(a.payout) || 0));   // bid: highest payout first
  }"""),

# 2. empty-eligible hold message
("""    const why = nldMissing.length && NLD_ONLY_STATES.includes(leadState) ? `Missing fields for buyer: ${nldMissing.join(', ')}` : `No eligible buyer for ${leadState} right now (daily caps reached)`;""",
"""    const why = isNldState ? (nldMissing.length ? `NLD needs: ${nldMissing.join(', ')} - held for your review` : `NLD could not take it right now - held for your review`) : `No eligible buyer for ${leadState} right now`;   // patch 379"""),

# 3. final write: hold on NLD-state non-accept
("""  const nldTry = attempts.find(a => a.buyer === 'NLD CPA');
  const c2 = await pool.connect();
  try {
    await c2.query(
      `UPDATE leads SET
         status          = $1,
         buyer_status    = $2,
         buyer_response  = $3::jsonb,
         billable        = false,
         revenue         = 0,
         raw             = COALESCE(raw,'{}'::jsonb) || $4::jsonb
       WHERE id = $5`,
      [accepted ? 'forwarded' : 'buyer_rejected',
       accepted ? 'Accepted' : 'Rejected',
       JSON.stringify({ final: result, attempts, ladder: ladderPlan, nld_attempted: !!nldTry, nld_response: nldTry ? (nldTry.response || nldTry.error) : null }),
       JSON.stringify({ buyer_name: buyerName, routing_attempts: attempts.map(a => a.buyer + (a.accepted ? ':accepted' : ':rejected')) }), leadId]
    );
  } finally { c2.release(); }""",
"""  const nldTry = attempts.find(a => a.buyer === 'NLD CPA');
  const heldForReview = !accepted && isNldState;   // patch 379: NLD declined on its state -> hold
  const heldReason = heldForReview
    ? ('NLD did not take it - held for your review' + (nldTry && nldTry.response && nldTry.response.message ? ': ' + nldTry.response.message : (nldMissing.length ? ': needs ' + nldMissing.join(', ') : '')))
    : null;
  const c2 = await pool.connect();
  try {
    await c2.query(
      `UPDATE leads SET
         status          = $1,
         buyer_status    = $2,
         buyer_response  = $3::jsonb,
         buyer_error     = $6,
         billable        = false,
         revenue         = 0,
         raw             = COALESCE(raw,'{}'::jsonb) || $4::jsonb
       WHERE id = $5`,
      [accepted ? 'forwarded' : (heldForReview ? 'received' : 'buyer_rejected'),
       accepted ? 'Accepted' : (heldForReview ? 'Held' : 'Rejected'),
       JSON.stringify({ final: result, attempts, ladder: ladderPlan, nld_attempted: !!nldTry, nld_response: nldTry ? (nldTry.response || nldTry.error) : null }),
       JSON.stringify({ buyer_name: accepted ? buyerName : (heldForReview ? null : buyerName), routing_attempts: attempts.map(a => a.buyer + (a.accepted ? ':accepted' : ':rejected')) }), leadId, heldReason]
    );
  } finally { c2.release(); }"""),

# 4. response result -> 'held'
("""    ok: accepted,
    result: accepted ? 'success' : 'rejected',
    message: result.message || (accepted ? 'Lead accepted' : 'Lead rejected'),""",
"""    ok: accepted,
    result: accepted ? 'success' : (isNldState ? 'held' : 'rejected'),
    message: result.message || (accepted ? 'Lead accepted' : (isNldState ? 'Held for your review' : 'Lead rejected')),"""),

# 5. janSend: add CAG + LAG
("""  if (buyer === 'DT-Intake') {   // patch 350
    const r = await sendToDtIntake(b, st, leadId, pub);
    return { result: r.result, accepted: r.accepted };
  }
  return { result: { error: 'unknown buyer' }, accepted: false };
}""",
"""  if (buyer === 'DT-Intake') {   // patch 350
    const r = await sendToDtIntake(b, st, leadId, pub);
    return { result: r.result, accepted: r.accepted };
  }
  if (buyer === 'CAG') {   // patch 379: bottom-tier bid on resend
    try { const r = await sendToCag(b, st, leadId); return { result: r.result, accepted: r.accepted }; }
    catch (e) { return { result: { status: 'error', message: 'CAG post failed: ' + e.message }, accepted: false }; }
  }
  if (buyer === 'Legal Advocate Group') {   // patch 379: bottom-tier bid on resend
    try { const out = await lagAppendRow(b, st, leadId); return { result: { status: 'success', message: 'Delivered to LAG sheet', range: out && out.updates && out.updates.updatedRange }, accepted: true }; }
    catch (e) { return { result: { status: 'error', message: 'LAG sheet append failed: ' + e.message }, accepted: false }; }
  }
  return { result: { error: 'unknown buyer' }, accepted: false };
}"""),

# 6. resend-ladder plan: NLD first, then price bid
("""    const rungs = [];
    const dtOn = DT_INTAKE_ON();
    const wantNld = !NLD_PAUSED && NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10;   // patch 318
    // patch 351: DT on its contracted states, NLD on its states, CH catches the rest
    if (dtOn && DT_INTAKE_STATES().includes(st)) rungs.push('DT-Intake');
    if (wantNld) rungs.push('NLD CPA');
    if (dtOn && !rungs.includes('DT-Intake')) rungs.push('DT-Intake');   // patch 359: DT catches the rest (CH off)
    if (typeof MVA003_LIVE === 'function' && MVA003_LIVE()) rungs.push('MVA-003-LT');   // patch 377: Eilers is the nationwide catch-all on resend too (bottom of the plan)
    let plan = rungs.filter((x, i) => rungs.indexOf(x) === i);   // dedupe, order kept""",
"""    // patch 379: push-through = NLD first on its states, then the bottom tier
    // (CAG / LAG / Eilers) by price point, highest payout first. DT/CH retired.
    const rungs = [];
    if (!NLD_PAUSED && NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10) rungs.push('NLD CPA');
    const bid = [];
    if (typeof CAG_LIVE === 'function' && CAG_LIVE() && CAG_STATES.includes(st)) bid.push({ name: 'CAG', payout: CAG_PAYOUT(st) });
    if (typeof LAG_LIVE === 'function' && LAG_LIVE() && LAG_STATES.includes(st)) bid.push({ name: 'Legal Advocate Group', payout: LAG_PAYOUT });
    if (typeof MVA003_LIVE === 'function' && MVA003_LIVE()) bid.push({ name: 'MVA-003-LT', payout: 1800 });
    bid.sort((a, b) => b.payout - a.payout).forEach(x => rungs.push(x.name));
    let plan = rungs.filter((x, i) => rungs.indexOf(x) === i);   // dedupe, order kept"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-379.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 379 applied")
