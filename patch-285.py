#!/usr/bin/env python3
"""
patch-285.py  --  Outreach board: safe "add one contact" endpoint + Kurt London

What it does to krw-backend/server.js:
  1. POST /outreach/contacts/add  (x-api-key, same as the other outreach routes)
     Adds or updates ONE contact without replacing the whole table (the existing
     PUT /outreach/contacts replaces everything). Matches on name+company so
     re-sending the same contact updates it instead of duplicating it.
     Body: {name, company?, role?, vertical?, stage?, source?, lastContact?,
            followup?, contact?, potential?, offer?, notes?}
     Missing id is assigned (max id + 1). Returns {ok, id, created}.
  2. One-time seed (runs once, then never again, tracked in krw_seed_flags):
     Kurt London / London Harker Injury Law, LinkedIn DM sent Oct 7 2026,
     stage Contacted, follow-up Oct 12.

Usage (from ~/krw/krw-backend):
    python3 patch-285.py
    node --check server.js
    git add -A && git commit -m "patch 285: outreach add-contact endpoint + Kurt London" && git push

A backup is written to server.js.pre-285.bak first. Safe to re-run: it refuses
to apply twice.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()

if "patch 285" in src:
    sys.exit("patch 285 already applied - nothing to do")

MARKER = "// ─── end patch 275 ────────────────────────────────────────────────────────────"
if src.count(MARKER) != 1:
    sys.exit("anchor for patch 275 not found exactly once - aborting, server.js untouched")

NEW = r'''
// ─── patch 285: add ONE outreach contact without replacing the table ──────────
// PUT /outreach/contacts replaces the whole board; this adds/updates a single
// contact so scripts, Claude, or a phone call can log outreach safely.
async function orUpsertContact(c) {
  await orEnsureTable();
  const name = String(c.name || '').trim();
  if (!name) throw new Error('name is required');
  const company = String(c.company || '').trim();
  const existing = await pool.query(
    `SELECT id, data FROM outreach_contacts
      WHERE lower(data->>'name') = lower($1) AND lower(COALESCE(data->>'company','')) = lower($2) LIMIT 1`,
    [name, company]);
  if (existing.rows.length) {
    const id = Number(existing.rows[0].id);
    const merged = Object.assign({}, existing.rows[0].data, c, { id, name, company });
    await pool.query('UPDATE outreach_contacts SET data=$2::jsonb, updated_at=NOW() WHERE id=$1', [id, JSON.stringify(merged)]);
    return { id, created: false };
  }
  const mx = await pool.query('SELECT COALESCE(MAX(id),0) AS m FROM outreach_contacts');
  const id = Number(mx.rows[0].m) + 1;
  const row = Object.assign({
    company: '', role: 'buyer', vertical: '', stage: 'cold', source: '',
    lastContact: '', followup: '', contact: '', potential: 0, offer: '', notes: ''
  }, c, { id, name, company });
  await pool.query('INSERT INTO outreach_contacts (id, data, updated_at) VALUES ($1, $2::jsonb, NOW())', [id, JSON.stringify(row)]);
  return { id, created: true };
}
app.post('/outreach/contacts/add', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    const r = await orUpsertContact(req.body || {});
    res.json(Object.assign({ ok: true }, r));
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// one-time seed (flag stored so a deleted contact is never re-created on redeploy)
async function orSeedOnce(flag, contacts) {
  await pool.query(`CREATE TABLE IF NOT EXISTS krw_seed_flags (flag TEXT PRIMARY KEY, done_at TIMESTAMPTZ DEFAULT NOW())`);
  const done = await pool.query('SELECT 1 FROM krw_seed_flags WHERE flag=$1', [flag]);
  if (done.rows.length) return;
  for (const c of contacts) await orUpsertContact(c);
  await pool.query('INSERT INTO krw_seed_flags (flag) VALUES ($1) ON CONFLICT DO NOTHING', [flag]);
  console.log('[Outreach] seeded ' + flag + ' (' + contacts.length + ')');
}
setTimeout(() => {
  orSeedOnce('patch285-kurt-london', [{
    name: 'Kurt London',
    company: 'London Harker Injury Law',
    role: 'buyer',
    vertical: 'MVA',
    stage: 'contacted',
    source: 'LinkedIn',
    lastContact: '2026-10-07',
    followup: '2026-10-12',
    contact: '',
    potential: 0,
    offer: 'PI leads (paid social, search, SEO) - Managing Partner, Utah PI firm also serving MT, AZ, CA, NV',
    notes: 'LinkedIn DM sent Oct 7 2026. Utah connection (Kyler grew up there). Casual pitch, wants to earn some of his business. https://www.linkedin.com/in/kurtlondon/'
  }]).catch(e => console.warn('[Outreach] patch 285 seed failed:', e.message));
}, 8000);
console.log('[Outreach] POST /outreach/contacts/add ready (patch 285)');
// ─── end patch 285 ────────────────────────────────────────────────────────────
'''

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-285.bak"))
out = src.replace(MARKER, MARKER + "\n" + NEW, 1)
open(TARGET, "w", encoding="utf-8").write(out)
print("patch 285 applied. backup: server.js.pre-285.bak")
print("next: node --check server.js && git add -A && git commit -m 'patch 285' && git push")
