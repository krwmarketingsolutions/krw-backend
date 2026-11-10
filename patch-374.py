#!/usr/bin/env python3
r"""
patch-374.py  --  batch TrustedForm lookup by phone (read-only)

For packaging aged-lead lists with their TrustedForm certificates. POST
/leads/trustedform-lookup { phones: [...] } matches each phone to its lead by
the last 10 digits and returns the TrustedForm cert URL from the lead's raw
data (raw->>'trustedform_cert_url', fallback raw->>'trusted_form_cert_url').
Read-only, admin-auth. Reusable for any future aged-lead export.

  krw-backend/server.js (one insert, before the CALL/REVENUE section).

Backup: server.js.pre-374.bak    Safe to re-run: refuses twice.
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
if "patch 374" in src:
    sys.exit("patch 374 already applied - nothing to do")

ENDPOINT = r"""// ── patch 374: batch TrustedForm lookup by phone (read-only) ─────────────────
app.post('/leads/trustedform-lookup', requireKey, async (req, res) => {
  try {
    const phones = Array.isArray(req.body && req.body.phones) ? req.body.phones : [];
    const p10s = [...new Set(phones.map(p => String(p || '').replace(/\D/g, '').slice(-10)).filter(p => p.length === 10))];
    if (!p10s.length) return res.json({ ok: true, count: 0, matched: 0, withCert: 0, results: [] });
    const r = await pool.query(
      `SELECT right(regexp_replace(phone, '\\D', '', 'g'), 10) AS p10,
              id, first_name, last_name, status, received_at,
              COALESCE(NULLIF(raw->>'trustedform_cert_url', ''), NULLIF(raw->>'trusted_form_cert_url', '')) AS tf
         FROM leads
        WHERE right(regexp_replace(phone, '\\D', '', 'g'), 10) = ANY($1::text[])
        ORDER BY (COALESCE(NULLIF(raw->>'trustedform_cert_url', ''), NULLIF(raw->>'trusted_form_cert_url', '')) IS NOT NULL) DESC, received_at DESC`,
      [p10s]);
    const map = {};
    for (const row of r.rows) { if (!map[row.p10]) map[row.p10] = row; }
    const results = p10s.map(p => ({
      phone: p,
      trustedform: (map[p] && map[p].tf) || '',
      krw_id: (map[p] && map[p].id) || null,
      status: (map[p] && map[p].status) || '',
      matched: !!map[p],
    }));
    return res.json({ ok: true, count: results.length, matched: results.filter(x => x.matched).length, withCert: results.filter(x => x.trustedform).length, results });
  } catch (e) { return res.status(500).json({ ok: false, error: e.message }); }
});

"""
anchor = ("// ══════════════════════════════════════════════════════\n"
          "//  CALL / REVENUE ENDPOINTS (existing TrackDrive flow)")
if src.count(anchor) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1." % src.count(anchor))
src = src.replace(anchor, ENDPOINT + anchor, 1)
shutil.copyfile(FILE, FILE + ".pre-374.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 374 applied to", FILE)
