#!/usr/bin/env python3
r"""
patch-356.py  --  publisher-side payout hold (raw.pub_hold)

Lets a signed lead stay billable for the BUYER (NLD reconciliation, admin
dashboard, revenue) while being held OFF the PUBLISHER's payout portal. Needed
because one `billable` flag drove both sides, so there was no way to owe NLD for
a lead yet hold the Leadbloom payout on it.

  krw-backend/server.js
    - pbLeadViewBase: a lead with raw.pub_hold=true returns payout 0, billable
      false in the portal view, and shows "In outreach" (not Signed), so the
      portal's Signed count and Payout exclude it. DB billable flag is untouched.
    - new admin route POST /leads/:id/pub-hold {hold:true|false}.

Backup: server.js.pre-356.bak    Safe to re-run: refuses twice.
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
if "patch 356" in src or "pub_hold" in src:
    sys.exit("patch 356 already applied - nothing to do")

edits = []

# A1 - define pubHold and drop it out of the "Signed" response
edits.append((
"  const raw = l.raw || {};\n"
"  const dispo = raw.buyer_disposition || {};",
"  const raw = l.raw || {};\n"
"  const pubHold = !!raw.pub_hold;   // patch 356: held off publisher payout, still billable for the buyer\n"
"  if (pubHold) response = 'In outreach';\n"
"  const dispo = raw.buyer_disposition || {};"))

# A2 - zero the payout and portal-billable for a held lead
edits.append((
"    updated_at: dispo.confirmed_at || dispo.synced_at || null, billable: !!l.billable,\n"
"    payout: l.billable ? parseFloat(payoutRate || 0) : 0,",
"    updated_at: dispo.confirmed_at || dispo.synced_at || null, billable: !!l.billable && !pubHold,\n"
"    payout: (l.billable && !pubHold) ? parseFloat(payoutRate || 0) : 0,"))

# B - admin endpoint to set/clear the hold
edits.append((
"app.post('/portal/leads/:id/hide',   requireKey, (req, res) => pbSetHidden(req, res, true));\n"
"app.post('/portal/leads/:id/unhide', requirePortalKey, (req, res) => pbSetHidden(req, res, false));",
"app.post('/portal/leads/:id/hide',   requireKey, (req, res) => pbSetHidden(req, res, true));\n"
"app.post('/portal/leads/:id/unhide', requirePortalKey, (req, res) => pbSetHidden(req, res, false));\n"
"\n"
"// patch 356: hold a lead off the publisher payout only (still billable for the buyer/dashboard)\n"
"app.post('/leads/:id/pub-hold', requireKey, async (req, res) => {\n"
"  const id = parseInt(req.params.id, 10);\n"
"  if (isNaN(id)) return res.status(400).json({ ok: false, error: 'Bad lead id' });\n"
"  const hold = ((req.body || {}).hold !== false);\n"
"  try {\n"
"    const r = await pool.query(\n"
"      `UPDATE leads SET raw = COALESCE(raw,'{}'::jsonb) || jsonb_build_object('pub_hold', $1::boolean, 'pub_hold_at', NOW())\n"
"       WHERE id = $2 RETURNING id, first_name, last_name, billable`,\n"
"      [hold, id]);\n"
"    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'Lead not found' });\n"
"    console.log(`[Portal] lead ${id} pub_hold -> ${hold}`);\n"
"    res.json({ ok: true, id: id, pub_hold: hold, lead: r.rows[0] });\n"
"  } catch (err) { res.status(500).json({ ok: false, error: err.message }); }\n"
"});"))

for label, (a, r) in zip(("A1", "A2", "B"), edits):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: anchor %s found %d times, expected 1. No change." % (label, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-356.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 356 applied to", FILE)
