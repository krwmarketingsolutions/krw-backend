#!/usr/bin/env python3
"""
patch-287.py  --  Outreach email sequence engine with an approval queue

Adds to krw-backend/server.js a cold-email engine for the Outreach board.
It writes drafts; it does NOT send anything until two env vars are set AND
you approve each draft. Nothing can go out by accident.

WHAT IT ADDS
  Tables
    outreach_sequence_state  one row per enrolled contact: step, next due, status
    outreach_drafts          one row per generated email, pending your approval
    outreach_suppression     unsubscribes + bounces; checked before every send

  Sequences (4 steps at day 0, 3, 7, 12), one set for MVA and one for SSDI.
  Merge fields: {{first_name}} {{firm}} {{state}} — a missing field degrades to
  a sentence that still reads naturally, never an empty "Hi ,".

  Generator runs 09:10 ET each weekday: for every enrolled contact whose next
  step is due, it writes ONE draft into the approval queue. It never sends.

  Routes (all need the admin x-api-key)
    POST /outreach/sequence/enroll   {contact_ids:[]} or {vertical,state,limit,stage}
    POST /outreach/sequence/stop     {contact_id, reason}
    GET  /outreach/sequence/status
    POST /outreach/sequence/run      generate drafts now (same as the 09:10 job)
    GET  /outreach/drafts?status=pending
    POST /outreach/drafts/approve    {ids:[...]} -> sends those, advances the step
    POST /outreach/drafts/skip       {ids:[...], reason}
    GET  /outreach/unsubscribe?t=    public, no key: one click, adds to suppression

  SAFETY RAILS, all on by default
    * Sending is OFF until OUTREACH_SEND=true AND OUTREACH_FROM are both set.
      Until then approve marks a draft 'approved' and sends nothing.
    * Every send checks: suppression list, the contact's own emailStatus
      (replied/meeting = sequence stops itself), a daily cap, and a dedupe on
      (contact, step).
    * Every email gets an unsubscribe link and your business address in the
      footer. Reply-To is your real inbox so replies reach you, not the
      sending domain.
    * OUTREACH_DAILY_CAP (default 40) is a hard stop per calendar day ET.

  ENV TO SET ON RAILWAY WHEN YOU ARE READY TO SEND
    OUTREACH_FROM      "Kyler Walterson <kyler@krw-outreach.com>"  (new domain)
    OUTREACH_REPLY_TO  kyler@krwmarketingsolutions.com
    OUTREACH_ADDRESS   your business mailing address, one line (CAN-SPAM)
    OUTREACH_SEND      true          <- the master switch, set this LAST
    OUTREACH_DAILY_CAP 40            optional
    PUBLIC_BASE_URL    https://krw-backend-production.up.railway.app

Usage (from ~/krw/krw-backend):
    python3 patch-287.py
    node --check server.js
    git add -A && git commit -m "patch 287: outreach sequence engine" && git push

Backup: server.js.pre-287.bak. Safe to re-run: refuses to apply twice.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 287" in src:
    sys.exit("patch 287 already applied - nothing to do")

ANCHORS = ["// ─── end patch 285 ────────────────────────────────────────────────────────────",
           "// ─── end patch 275 ────────────────────────────────────────────────────────────"]
anchor = next((a for a in ANCHORS if src.count(a) == 1), None)
if not anchor:
    sys.exit("no unique outreach anchor found (run patch 285 first) - aborting")

NEW = r'''
// ─── patch 287: outreach email sequences + approval queue ─────────────────────
// Drafts are generated on a schedule; nothing is sent until OUTREACH_SEND=true
// and OUTREACH_FROM are set AND a draft is explicitly approved.
const OZ_SEND_ON   = () => String(process.env.OUTREACH_SEND || '').toLowerCase() === 'true';
const OZ_FROM      = () => process.env.OUTREACH_FROM || '';
const OZ_REPLY_TO  = () => process.env.OUTREACH_REPLY_TO || process.env.NOTIFY_EMAIL || 'kyler@leadbloom.co';
const OZ_ADDRESS   = () => process.env.OUTREACH_ADDRESS || 'KRW Marketing Solutions LLC, Los Angeles, CA';
const OZ_CAP       = () => parseInt(process.env.OUTREACH_DAILY_CAP || '40', 10);
const OZ_BASE      = () => (process.env.PUBLIC_BASE_URL || 'https://krw-backend-production.up.railway.app').replace(/\/+$/, '');

// steps: day offset from enrolment. Keep the gaps; they are what stops this
// reading like a blast.
const OZ_STEPS = [0, 3, 7, 12];

const OZ_SEQ = {
  MVA: [
    { subject: 'quick question{{, first_name}}',
      body: "{{Hey_name}},\n\nI run KRW Marketing Solutions. We generate MVA leads through paid social, search and SEO{{, and we have been sending a lot of state volume lately}}. {{firm_or_you}} came up as somewhere that might want more of it.\n\nAre you taking on new MVA cases right now? If so I'd love to send over how our leads work and see if it's a fit.\n\nKyler" },
    { subject: 're: quick question',
      body: "{{Hey_name}}, bumping this in case it got buried.\n\nHappy to keep it short — 15 minutes and you can tell me if it makes sense or not.\n\nKyler" },
    { subject: 'how we do MVA leads',
      body: "{{Name_or_Hey_there}}, a bit more on what we do.\n\nEvery lead comes in with a TrustedForm certificate, we only send leads in the states you take, and you can send back anything that doesn't qualify.\n\nWant me to set up a small test batch so you can see the lead quality yourself?\n\nKyler" },
    { subject: 'closing the loop',
      body: "{{Hey_name}}, I'll stop bugging you after this one.\n\nIf MVA leads aren't a priority right now, no worries at all. If that changes, just reply and I'll get you set up quick.\n\nKyler" }
  ],
  SSDI: [
    { subject: 'quick question{{, first_name}}',
      body: "{{Hey_name}},\n\nI run KRW Marketing Solutions. We generate SSDI claimant leads and transfers, and work with firms taking disability cases{{ in state}}.\n\nIs {{firm_or_your_team}} taking new SSDI claimants right now? If so I'd love to show you what our volume looks like.\n\nKyler" },
    { subject: 're: quick question',
      body: "{{Hey_name}}, bumping this one.\n\n15 minutes and you can tell me if it's a fit or not.\n\nKyler" },
    { subject: 'how our SSDI leads work',
      body: "{{Name_or_Hey_there}}, a little more detail.\n\nClaimants are pre-screened before they reach you, every lead carries a consent record, and you only pay on what you take.\n\nWorth a short call to see if the volume fits what you're working?\n\nKyler" },
    { subject: 'closing the loop',
      body: "{{Hey_name}}, last one from me.\n\nIf SSDI isn't where you're putting money right now that's completely fine. Reply any time and I'll pick it back up.\n\nKyler" }
  ]
};
function ozSeqFor(vertical) {
  return String(vertical || '').toUpperCase().indexOf('SSDI') > -1 ? OZ_SEQ.SSDI : OZ_SEQ.MVA;
}
function ozFirstName(c) {
  const n = String(c.name || '').trim();
  if (!n || n.indexOf('@') > -1) return '';
  const firm = String(c.company || '').trim();
  if (firm && n.toLowerCase() === firm.toLowerCase()) return '';   // firm-only row
  if (/\b(law|legal|llp|llc|pllc|firm|group|associates|injury|attorneys?|partners|offices?)\b/i.test(n)) return '';
  const w = n.split(/\s+/)[0];
  return /^[A-Za-z][A-Za-z'`-]{1,}$/.test(w) ? w : '';
}
// Fill merge fields so a missing value never leaves a hole in a sentence.
function ozFill(tpl, c) {
  const fn = ozFirstName(c), firm = String(c.company || '').trim(), st = String(c.state || '').trim();
  return String(tpl)
    .replace(/\{\{, first_name\}\}/g, fn ? ', ' + fn : '')
    .replace(/\{\{Hey_name\}\}/g, fn ? 'Hey ' + fn : 'Hey there')
    .replace(/\{\{Name_or_Hey_there\}\}/g, fn ? fn : 'Hey there')
    .replace(/\{\{firm_or_you\}\}/g, firm || 'Your firm')
    .replace(/\{\{firm_or_your_team\}\}/g, firm || 'your team')
    .replace(/\{\{, and we have been sending a lot of state volume lately\}\}/g,
             st ? ', and we have been sending a lot of ' + st + ' volume lately' : '')
    .replace(/\{\{ in state\}\}/g, st ? ' in ' + st : '')
    .replace(/\{\{firm\}\}/g, firm)
    .replace(/\{\{state\}\}/g, st)
    .replace(/\{\{first_name\}\}/g, fn)
    // tidy spacing WITHOUT touching newlines - \s would eat the blank lines
    // between paragraphs and turn the email into one block of text
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}
function ozToken(email) {
  return require('crypto').createHmac('sha256', process.env.API_KEY || 'krw-outreach')
    .update(String(email).toLowerCase()).digest('hex').slice(0, 24);
}
function ozHtml(bodyText, email) {
  const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const link = OZ_BASE() + '/outreach/unsubscribe?e=' + encodeURIComponent(email) + '&t=' + ozToken(email);
  return '<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.55;color:#1a1a18">' +
    esc(bodyText).split('\n').map(l => l.trim() === '' ? '<br>' : '<p style="margin:0 0 12px">' + l + '</p>').join('') +
    '<hr style="border:0;border-top:1px solid #e0ded9;margin:20px 0 10px">' +
    '<p style="margin:0;font-size:11px;color:#8a8880">' + esc(OZ_ADDRESS()) +
    ' · <a href="' + link + '" style="color:#8a8880">Unsubscribe</a></p></div>';
}

let ozReady = false;
async function ozEnsure() {
  if (ozReady) return;
  await pool.query(`CREATE TABLE IF NOT EXISTS outreach_sequence_state (
    contact_id BIGINT PRIMARY KEY, email TEXT, vertical TEXT,
    step INT NOT NULL DEFAULT 0, next_due DATE,
    status TEXT NOT NULL DEFAULT 'active', stop_reason TEXT,
    enrolled_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
  await pool.query(`CREATE TABLE IF NOT EXISTS outreach_drafts (
    id SERIAL PRIMARY KEY, contact_id BIGINT NOT NULL, step INT NOT NULL,
    to_email TEXT NOT NULL, to_name TEXT, firm TEXT,
    subject TEXT NOT NULL, body TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(), sent_at TIMESTAMPTZ)`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS outreach_drafts_uniq ON outreach_drafts (contact_id, step)`);
  await pool.query(`CREATE TABLE IF NOT EXISTS outreach_suppression (
    email TEXT PRIMARY KEY, reason TEXT, added_at TIMESTAMPTZ DEFAULT NOW())`);
  ozReady = true;
}
async function ozContacts(ids) {
  await orEnsureTable();
  const r = ids && ids.length
    ? await pool.query('SELECT id, data FROM outreach_contacts WHERE id = ANY($1::bigint[])', [ids])
    : await pool.query('SELECT id, data FROM outreach_contacts');
  return r.rows.map(x => Object.assign({}, x.data, { id: Number(x.id) }));
}
function ozEmailOf(c) { const v = String(c.contact || '').trim(); return v.indexOf('@') > 0 ? v : ''; }

// ── enrol ─────────────────────────────────────────────────────────────────────
app.post('/outreach/sequence/enroll', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await ozEnsure();
    const b = req.body || {};
    let list = await ozContacts(Array.isArray(b.contact_ids) ? b.contact_ids.map(Number) : null);
    if (!Array.isArray(b.contact_ids)) {
      if (b.vertical) list = list.filter(c => String(c.vertical || '').toUpperCase().indexOf(String(b.vertical).toUpperCase()) > -1);
      if (b.state)    list = list.filter(c => String(c.state || '').toUpperCase() === String(b.state).toUpperCase());
      if (b.stage)    list = list.filter(c => c.stage === b.stage);
      if (b.limit)    list = list.slice(0, Math.max(0, parseInt(b.limit, 10) || 0));
    }
    const sup = (await pool.query('SELECT email FROM outreach_suppression')).rows.map(r => r.email.toLowerCase());
    const out = { enrolled: 0, skipped_no_email: 0, skipped_suppressed: 0, skipped_replied: 0, already: 0 };
    for (const c of list) {
      const em = ozEmailOf(c);
      if (!em) { out.skipped_no_email++; continue; }
      if (sup.indexOf(em.toLowerCase()) > -1) { out.skipped_suppressed++; continue; }
      if (c.emailStatus === 'replied' || c.emailStatus === 'meeting') { out.skipped_replied++; continue; }
      const ex = await pool.query('SELECT 1 FROM outreach_sequence_state WHERE contact_id=$1', [c.id]);
      if (ex.rows.length) { out.already++; continue; }
      await pool.query(
        `INSERT INTO outreach_sequence_state (contact_id, email, vertical, step, next_due, status)
         VALUES ($1,$2,$3,0,(NOW() AT TIME ZONE 'America/New_York')::date,'active')`,
        [c.id, em, c.vertical || '']);
      out.enrolled++;
    }
    res.json(Object.assign({ ok: true }, out));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/outreach/sequence/stop', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await ozEnsure();
    const id = Number((req.body || {}).contact_id);
    if (!id) return res.status(400).json({ ok: false, error: 'contact_id required' });
    await pool.query(`UPDATE outreach_sequence_state SET status='stopped', stop_reason=$2, updated_at=NOW() WHERE contact_id=$1`,
      [id, String((req.body || {}).reason || 'stopped by hand')]);
    await pool.query(`UPDATE outreach_drafts SET status='skipped' WHERE contact_id=$1 AND status='pending'`, [id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── generate drafts (never sends) ─────────────────────────────────────────────
async function ozGenerate() {
  await ozEnsure();
  const due = await pool.query(
    `SELECT * FROM outreach_sequence_state
      WHERE status='active' AND next_due IS NOT NULL
        AND next_due <= (NOW() AT TIME ZONE 'America/New_York')::date`);
  if (!due.rows.length) return { generated: 0, stopped: 0, checked: 0 };
  const byId = {};
  (await ozContacts(due.rows.map(r => Number(r.contact_id)))).forEach(c => { byId[c.id] = c; });
  const sup = (await pool.query('SELECT email FROM outreach_suppression')).rows.map(r => r.email.toLowerCase());
  let generated = 0, stopped = 0;
  for (const st of due.rows) {
    const c = byId[Number(st.contact_id)];
    // the sequence polices itself off the board: a reply or a meeting ends it
    if (!c || c.emailStatus === 'replied' || c.emailStatus === 'meeting') {
      await pool.query(`UPDATE outreach_sequence_state SET status='stopped', stop_reason=$2, updated_at=NOW() WHERE contact_id=$1`,
        [st.contact_id, !c ? 'contact removed from the board' : 'they replied']);
      stopped++; continue;
    }
    const em = ozEmailOf(c);
    if (!em || sup.indexOf(em.toLowerCase()) > -1) {
      await pool.query(`UPDATE outreach_sequence_state SET status='stopped', stop_reason=$2, updated_at=NOW() WHERE contact_id=$1`,
        [st.contact_id, !em ? 'no email on the contact' : 'unsubscribed']);
      stopped++; continue;
    }
    const seq = ozSeqFor(c.vertical);
    const step = Number(st.step) || 0;
    if (step >= seq.length) {
      await pool.query(`UPDATE outreach_sequence_state SET status='done', updated_at=NOW() WHERE contact_id=$1`, [st.contact_id]);
      continue;
    }
    const tpl = seq[step];
    const r = await pool.query(
      `INSERT INTO outreach_drafts (contact_id, step, to_email, to_name, firm, subject, body, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'pending') ON CONFLICT (contact_id, step) DO NOTHING RETURNING id`,
      [c.id, step, em, c.name || '', c.company || '', ozFill(tpl.subject, c), ozFill(tpl.body, c)]);
    if (r.rows.length) generated++;
  }
  return { generated, stopped, checked: due.rows.length };
}
app.post('/outreach/sequence/run', async (req, res) => {
  if (!orAuth(req, res)) return;
  try { res.json(Object.assign({ ok: true }, await ozGenerate())); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/outreach/sequence/status', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await ozEnsure();
    const s = await pool.query(`SELECT status, COUNT(*)::int n FROM outreach_sequence_state GROUP BY status`);
    const d = await pool.query(`SELECT status, COUNT(*)::int n FROM outreach_drafts GROUP BY status`);
    const sentToday = await pool.query(
      `SELECT COUNT(*)::int n FROM outreach_drafts WHERE status='sent'
        AND (sent_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date`);
    res.json({ ok: true,
      sending_enabled: OZ_SEND_ON() && !!OZ_FROM(),
      from: OZ_FROM() || null, reply_to: OZ_REPLY_TO(),
      daily_cap: OZ_CAP(), sent_today: sentToday.rows[0].n,
      sequences: s.rows, drafts: d.rows,
      blocked_reason: OZ_SEND_ON() ? (OZ_FROM() ? null : 'OUTREACH_FROM is not set') : 'OUTREACH_SEND is not true' });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/outreach/drafts', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await ozEnsure();
    const st = req.query.status || 'pending';
    const r = await pool.query(
      `SELECT * FROM outreach_drafts ${st === 'all' ? '' : 'WHERE status=$1'} ORDER BY created_at DESC, id DESC LIMIT 500`,
      st === 'all' ? [] : [st]);
    res.json({ ok: true, drafts: r.rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/outreach/drafts/skip', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await ozEnsure();
    const ids = ((req.body || {}).ids || []).map(Number).filter(Boolean);
    if (!ids.length) return res.status(400).json({ ok: false, error: 'ids required' });
    await pool.query(`UPDATE outreach_drafts SET status='skipped', error=$2 WHERE id = ANY($1::int[]) AND status='pending'`,
      [ids, String((req.body || {}).reason || '')]);
    // skipping still advances the sequence so it doesn't jam on that step
    const rows = (await pool.query('SELECT contact_id, step FROM outreach_drafts WHERE id = ANY($1::int[])', [ids])).rows;
    for (const r of rows) await ozAdvance(Number(r.contact_id), Number(r.step));
    res.json({ ok: true, skipped: ids.length });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

async function ozAdvance(contactId, step) {
  const st = (await pool.query('SELECT * FROM outreach_sequence_state WHERE contact_id=$1', [contactId])).rows[0];
  if (!st) return;
  const c = (await ozContacts([contactId]))[0] || {};
  const seq = ozSeqFor(c.vertical || st.vertical);
  const next = step + 1;
  if (next >= seq.length) {
    await pool.query(`UPDATE outreach_sequence_state SET step=$2, status='done', updated_at=NOW() WHERE contact_id=$1`, [contactId, next]);
    return;
  }
  const gap = OZ_STEPS[next] - OZ_STEPS[step];
  await pool.query(
    `UPDATE outreach_sequence_state SET step=$2,
       next_due=((NOW() AT TIME ZONE 'America/New_York')::date + ($3 || ' days')::interval)::date,
       updated_at=NOW() WHERE contact_id=$1`,
    [contactId, next, String(gap > 0 ? gap : 1)]);
}

// ── approve = the only path that sends ────────────────────────────────────────
app.post('/outreach/drafts/approve', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await ozEnsure();
    const ids = ((req.body || {}).ids || []).map(Number).filter(Boolean);
    if (!ids.length) return res.status(400).json({ ok: false, error: 'ids required' });
    const canSend = OZ_SEND_ON() && !!OZ_FROM() && !!process.env.RESEND_API_KEY;
    if (!canSend) {
      await pool.query(`UPDATE outreach_drafts SET status='approved' WHERE id = ANY($1::int[]) AND status='pending'`, [ids]);
      return res.json({ ok: true, sent: 0, approved: ids.length, sending_enabled: false,
        note: 'Marked approved but NOT sent. Set OUTREACH_FROM, RESEND_API_KEY and OUTREACH_SEND=true on Railway, then approve again.' });
    }
    const capRow = await pool.query(
      `SELECT COUNT(*)::int n FROM outreach_drafts WHERE status='sent'
        AND (sent_at AT TIME ZONE 'America/New_York')::date = (NOW() AT TIME ZONE 'America/New_York')::date`);
    let room = Math.max(0, OZ_CAP() - capRow.rows[0].n);
    const rows = (await pool.query(
      `SELECT * FROM outreach_drafts WHERE id = ANY($1::int[]) AND status IN ('pending','approved') ORDER BY id`, [ids])).rows;
    const sup = (await pool.query('SELECT email FROM outreach_suppression')).rows.map(r => r.email.toLowerCase());
    const out = { sent: 0, failed: 0, held_by_cap: 0, suppressed: 0, errors: [] };
    for (const d of rows) {
      if (room <= 0) { out.held_by_cap++; continue; }
      if (sup.indexOf(String(d.to_email).toLowerCase()) > -1) {
        await pool.query(`UPDATE outreach_drafts SET status='skipped', error='unsubscribed' WHERE id=$1`, [d.id]);
        out.suppressed++; continue;
      }
      const c = (await ozContacts([Number(d.contact_id)]))[0];
      if (c && (c.emailStatus === 'replied' || c.emailStatus === 'meeting')) {
        await pool.query(`UPDATE outreach_drafts SET status='skipped', error='they replied first' WHERE id=$1`, [d.id]);
        continue;
      }
      try {
        const r = await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from: OZ_FROM(), to: [d.to_email], reply_to: OZ_REPLY_TO(),
            subject: d.subject, text: d.body + '\n\n—\n' + OZ_ADDRESS() +
              '\nUnsubscribe: ' + OZ_BASE() + '/outreach/unsubscribe?e=' + encodeURIComponent(d.to_email) + '&t=' + ozToken(d.to_email),
            html: ozHtml(d.body, d.to_email)
          })
        });
        const j = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(JSON.stringify(j).slice(0, 300));
        await pool.query(`UPDATE outreach_drafts SET status='sent', sent_at=NOW(), error=NULL WHERE id=$1`, [d.id]);
        await ozAdvance(Number(d.contact_id), Number(d.step));
        // keep the board in step: bump the touch counters the Outreach page shows
        await pool.query(
          `UPDATE outreach_contacts SET data = data
             || jsonb_build_object('emailsSent', (COALESCE((data->>'emailsSent')::int,0) + 1),
                                   'lastEmailAt', to_char((NOW() AT TIME ZONE 'America/New_York')::date,'YYYY-MM-DD'),
                                   'lastContact', to_char((NOW() AT TIME ZONE 'America/New_York')::date,'YYYY-MM-DD'),
                                   'emailStatus', CASE WHEN COALESCE(data->>'emailStatus','') IN ('replied','meeting')
                                                       THEN data->>'emailStatus' ELSE 'sent' END,
                                   'stage', CASE WHEN COALESCE(data->>'stage','') = 'cold'
                                                 THEN 'contacted' ELSE data->>'stage' END),
                 updated_at = NOW()
           WHERE id=$1`, [Number(d.contact_id)]);
        out.sent++; room--;
      } catch (e) {
        await pool.query(`UPDATE outreach_drafts SET status='failed', error=$2 WHERE id=$1`, [d.id, e.message.slice(0, 400)]);
        out.failed++; out.errors.push({ id: d.id, error: e.message.slice(0, 200) });
      }
    }
    res.json(Object.assign({ ok: true, sending_enabled: true }, out));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── unsubscribe (public, no key) ──────────────────────────────────────────────
app.get('/outreach/unsubscribe', async (req, res) => {
  try {
    await ozEnsure();
    const em = String(req.query.e || '').trim();
    const t  = String(req.query.t || '').trim();
    if (!em || t !== ozToken(em)) return res.status(400).send('Invalid unsubscribe link.');
    await pool.query(`INSERT INTO outreach_suppression (email, reason) VALUES ($1,'unsubscribed')
                      ON CONFLICT (email) DO NOTHING`, [em]);
    await pool.query(
      `UPDATE outreach_sequence_state s SET status='stopped', stop_reason='unsubscribed', updated_at=NOW()
         WHERE lower(s.email)=lower($1)`, [em]);
    await pool.query(`UPDATE outreach_drafts SET status='skipped', error='unsubscribed'
                       WHERE lower(to_email)=lower($1) AND status IN ('pending','approved')`, [em]);
    res.set('Content-Type', 'text/html').send(
      '<html><body style="font-family:Arial,sans-serif;max-width:520px;margin:60px auto;line-height:1.6;color:#1a1a18">' +
      '<h2 style="font-weight:600">You are unsubscribed</h2>' +
      '<p>' + em.replace(/[<>&]/g, '') + ' will not receive any further email from us.</p>' +
      '<p style="font-size:12px;color:#8a8880">' + OZ_ADDRESS() + '</p></body></html>');
  } catch (e) { res.status(500).send('Something went wrong.'); }
});

// ── 09:10 ET weekday draft generation (generates only; never sends) ───────────
let ozLastGenDay = null;
setInterval(async () => {
  try {
    const now = new Date();
    const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const day = et.toISOString().slice(0, 10);
    const dow = et.getDay();
    if (dow === 0 || dow === 6) return;
    if (et.getHours() !== 9 || et.getMinutes() < 10) return;
    if (ozLastGenDay === day) return;
    ozLastGenDay = day;
    const r = await ozGenerate();
    if (r.generated) {
      await sendEmailNotification(
        'Outreach: ' + r.generated + ' draft' + (r.generated === 1 ? '' : 's') + ' waiting for approval',
        '<p>' + r.generated + ' outreach email' + (r.generated === 1 ? '' : 's') + ' are drafted and waiting on you.' +
        (r.stopped ? ' ' + r.stopped + ' sequence(s) stopped on their own.' : '') +
        '</p><p>Nothing sends until you approve it.</p>');
    }
    console.log('[Outreach] draft run', JSON.stringify(r));
  } catch (e) { console.error('[Outreach] draft run failed:', e.message); }
}, 5 * 60 * 1000);

console.log('[Outreach] sequence engine ready (patch 287) - sending ' +
  (OZ_SEND_ON() && OZ_FROM() ? 'ENABLED as ' + OZ_FROM() : 'OFF until OUTREACH_SEND=true and OUTREACH_FROM are set'));
// ─── end patch 287 ────────────────────────────────────────────────────────────
'''

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-287.bak"))
open(TARGET, "w", encoding="utf-8").write(src.replace(anchor, anchor + "\n" + NEW, 1))
print("patch 287 applied (anchored after %s). backup: server.js.pre-287.bak"
      % ("patch 285" if "285" in anchor else "patch 275"))
print("next: node --check server.js && git add -A && git commit -m 'patch 287' && git push")
