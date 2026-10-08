#!/usr/bin/env python3
"""
patch-314.py  --  Backend half of the outreach command center (pairs with 313)

WHAT IT ADDS
  1. GET/POST /outreach/templates - the dashboard's editable email and
     LinkedIn templates, stored server side (outreach_reply_state kv) so they
     follow Kyler across devices.
  2. The LinkedIn draft scanner (patch 310/312) now also downloads each draft
     email's body and saves the actual message text onto the contact
     (data.liMessage), powering the board's "Copy message" button.
  3. Reply sync: when a contact replies by email, their pending follow-up date
     is cleared too, so the Needs-you-today nudge never chases someone who
     already answered.
  4. Friday evening brief gains a "LinkedIn outreach this week" recap:
     messaged, responded (and rate), in talks, meetings set, gone quiet.

Usage (from ~/krw/krw-backend):
    python3 patch-314.py
    node --check server.js
    git add -A && git commit -m "patch 314: outreach backend" && git push

Backup: server.js.pre-314.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")
src = open(TARGET, encoding="utf-8").read()
if "patch 314" in src:
    sys.exit("patch 314 already applied - nothing to do")

def must(label, old, new):
    global src
    n = src.count(old)
    if n != 1:
        sys.exit("%s: expected 1 match, found %d - aborting, server.js untouched" % (label, n))
    src = src.replace(old, new, 1)

# ── 1. template endpoints ─────────────────────────────────────────────────────
must("templates endpoints",
"""app.post('/outreach/contacts/add', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    const r = await orUpsertContact(req.body || {});
    res.json(Object.assign({ ok: true }, r));
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});""",
"""app.post('/outreach/contacts/add', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    const r = await orUpsertContact(req.body || {});
    res.json(Object.assign({ ok: true }, r));
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

// patch 314: dashboard-editable outreach templates (intro/follow/bump emails
// plus the LinkedIn follow-up text). Stored server side so they follow Kyler
// across desktop and phone. Shape per template: { s: subject, b: body }.
app.get('/outreach/templates', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await replyEnsure();
    const out = {};
    for (const k of ['intro', 'follow', 'bump', 'li_follow']) {
      const v = await replyGet('oztpl_' + k, '');
      if (v) { try { out[k] = JSON.parse(v); } catch (e) {} }
    }
    res.json({ ok: true, templates: out });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/outreach/templates', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await replyEnsure();
    const t = (req.body || {}).templates || {};
    for (const k of ['intro', 'follow', 'bump', 'li_follow']) {
      if (t[k] && typeof t[k] === 'object' && (t[k].b || t[k].s)) {
        await replySet('oztpl_' + k, JSON.stringify({ s: String(t[k].s || '').slice(0, 300), b: String(t[k].b || '').slice(0, 4000) }));
      }
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});""")

# ── 2. scanner captures the drafted message text ──────────────────────────────
must("liExtractDraft helper",
"""async function scanLinkedInDrafts() {""",
"""// patch 314: pull the drafted message text out of the agent's email so the
// board's "Copy message" button has something to copy.
function liExtractDraft(source) {
  try {
    let s = source ? source.toString('utf8') : '';
    let cut = s.indexOf('\\r\\n\\r\\n'); if (cut < 0) cut = s.indexOf('\\n\\n');
    if (cut < 0) return '';
    const head = s.slice(0, cut), bodyRaw = s.slice(cut).trim();
    let body = bodyRaw;
    if (/content-transfer-encoding:\\s*base64/i.test(head)) {
      try { body = Buffer.from(bodyRaw.replace(/\\s+/g, ''), 'base64').toString('utf8'); } catch (e) {}
    } else if (/content-transfer-encoding:\\s*quoted-printable/i.test(head)) {
      body = bodyRaw.replace(/=\\r?\\n/g, '').replace(/=([0-9A-F]{2})/gi, function (m, h) { return String.fromCharCode(parseInt(h, 16)); });
    }
    const ix = body.search(/draft message:/i);
    if (ix < 0) return '';
    return body.slice(ix).replace(/^draft message:\\s*/i, '').trim().slice(0, 4000);
  } catch (e) { return ''; }
}
async function scanLinkedInDrafts() {""")

must("scanner fetch opts",
"""    const lock = await client.getMailboxLock('[Gmail]/Sent Mail');
    try {
      for await (const msg of client.fetch({ since }, { envelope: true, uid: true })) {""",
"""    const lock = await client.getMailboxLock('[Gmail]/Sent Mail');
    try {
      for await (const msg of client.fetch({ since }, { envelope: true, uid: true, source: true })) {""")

must("scanner existing-contact update",
"""        if (existing.rows.length) {
          await pool.query(
            `UPDATE outreach_contacts SET data = data
               || jsonb_build_object('liDraftAt', $2::text)
               || CASE WHEN COALESCE(data->>'source','') = '' THEN '{"source":"LinkedIn"}'::jsonb ELSE '{}'::jsonb END,
               updated_at = NOW() WHERE id = $1`, [Number(existing.rows[0].id), day]);
          refreshed++;
        } else {
          await orUpsertContact({ name, company, role: 'buyer', vertical: 'MVA', stage: 'cold',
            source: 'LinkedIn', liDraftAt: day, notes: 'LinkedIn draft ready (emailed ' + day + ')' });
          added++;
        }""",
"""        const liMsg = liExtractDraft(msg.source);   // patch 314
        if (existing.rows.length) {
          await pool.query(
            `UPDATE outreach_contacts SET data = data
               || jsonb_build_object('liDraftAt', $2::text)
               || CASE WHEN $3::text <> '' THEN jsonb_build_object('liMessage', $3::text) ELSE '{}'::jsonb END
               || CASE WHEN COALESCE(data->>'source','') = '' THEN '{"source":"LinkedIn"}'::jsonb ELSE '{}'::jsonb END,
               updated_at = NOW() WHERE id = $1`, [Number(existing.rows[0].id), day, liMsg || '']);
          refreshed++;
        } else {
          await orUpsertContact(Object.assign({ name, company, role: 'buyer', vertical: 'MVA', stage: 'cold',
            source: 'LinkedIn', liDraftAt: day, notes: 'LinkedIn draft ready (emailed ' + day + ')' },
            liMsg ? { liMessage: liMsg } : {}));
          added++;
        }""")

# ── 3. reply sync clears the follow-up nudge ──────────────────────────────────
must("reply clears followup",
"""      `UPDATE outreach_contacts SET data = data || jsonb_build_object(
         'emailStatus','replied',
         'lastContact', to_char((NOW() AT TIME ZONE 'America/New_York')::date,'YYYY-MM-DD'),""",
"""      `UPDATE outreach_contacts SET data = data || jsonb_build_object(
         'emailStatus','replied',
         'followup','',
         'lastContact', to_char((NOW() AT TIME ZONE 'America/New_York')::date,'YYYY-MM-DD'),""")

# ── 4. Friday evening LinkedIn recap ─────────────────────────────────────────
must("recap compute",
"""  const chase = slot === 'pm' ? await buildChaseSection() : '';""",
"""  const chase = slot === 'pm' ? await buildChaseSection() : '';
  // patch 314: Friday evening LinkedIn outreach recap
  let liRecap = '';
  try {
    const dowPT = new Date().toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles', weekday: 'short' });
    if (slot === 'pm' && dowPT === 'Fri') {
      const rows = (await pool.query(`SELECT data FROM outreach_contacts WHERE data->>'source' = 'LinkedIn'`)).rows.map(r => r.data);
      const since = Date.now() - 7 * 86400000;
      const inWeek = d => d && new Date(d).getTime() >= since;
      const adv = ['responded', 'intalks', 'meeting', 'ready'];
      const sent = rows.filter(c => inWeek(c.sentAt)).length;
      const resp = rows.filter(c => adv.includes(c.stage) && inWeek(c.lastContact)).length;
      const talks = rows.filter(c => c.stage === 'intalks').length;
      const meets = rows.filter(c => c.stage === 'meeting').length;
      const quiet = rows.filter(c => c.stage === 'contacted' && c.sentAt && !inWeek(c.sentAt)).length;
      liRecap = `<h3>LinkedIn outreach this week</h3><p>${sent} messaged, ${resp} responded` +
        (sent ? ` (${Math.round(100 * resp / sent)}% response)` : '') +
        `. ${talks} in talks, ${meets} meeting${meets === 1 ? '' : 's'} set. ` +
        (quiet ? `${quiet} gone quiet (messaged over a week ago, no response) — worth a follow up.` : 'Nobody has gone quiet.') + `</p>`;
    }
  } catch (e) { console.error('[Daily Brief] LinkedIn recap error:', e.message); }""")

must("recap render",
"""  if (chase) html += chase;
  if (!alerts.rows.length && !chase) html += '<p>No alerts. Nothing is waiting on you beyond the approval queue.</p>';""",
"""  if (chase) html += chase;
  if (liRecap) html += liRecap;
  if (!alerts.rows.length && !chase && !liRecap) html += '<p>No alerts. Nothing is waiting on you beyond the approval queue.</p>';""")

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-314.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 314 applied. backup: server.js.pre-314.bak")
