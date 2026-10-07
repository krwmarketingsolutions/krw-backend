#!/usr/bin/env python3
"""
patch-291.py  --  Outreach reply detection

THE GAP IT CLOSES
  Patch 287 stops a sequence the moment a contact is marked 'replied', but
  nothing marked them. Until now that was a button you had to press. This
  watches the inbox replies actually land in and does it for you.

HOW IT WORKS
  Every 15 minutes the server opens your Gmail over IMAP (read-only), looks at
  mail that arrived since the last check, and matches each sender against the
  email addresses on your Outreach board. A match means:
      - the contact is marked replied (emailStatus='replied')
      - their stage moves to 'responded' if it was behind that
      - their sequence stops, and any draft still pending is skipped
      - the reply is logged, and you get one email listing who replied
  Read-only: it never sends, deletes, moves or marks anything as read.

  Uses GMAIL_USER / GMAIL_PASS, which are already set on Railway but unused by
  any code. GMAIL_PASS must be a Google App Password, not your account
  password. If either is unset, or the imapflow package is unavailable, the
  watcher logs one line and stays off - the rest of the server is unaffected.

  Routes (admin key or dashboard token):
      POST /outreach/replies/scan     run a scan now
      GET  /outreach/replies/status   last scan, matches found, whether armed
      GET  /outreach/replies/log      the last 100 replies detected

  Env:
      GMAIL_USER            the mailbox to watch (already set)
      GMAIL_PASS            Google App Password (already set)
      REPLY_WATCH           set to 'false' to disable without removing code
      REPLY_LOOKBACK_DAYS   how far back the first scan reaches (default 3)

Usage (from ~/krw/krw-backend):
    python3 patch-291.py
    node --check server.js
    git add -A && git commit -m "patch 291: outreach reply detection" && git push

Backup: server.js.pre-291.bak. Safe to re-run.
"""
import json, os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 291" in src:
    sys.exit("patch 291 already applied - nothing to do")

anchor = "// ─── end patch 287 ────────────────────────────────────────────────────────────"
if src.count(anchor) != 1:
    sys.exit("patch 287 anchor not found - run patch 287 first")

NEW = r'''
// ─── patch 291: watch the inbox for replies and stop those sequences ──────────
// Read-only IMAP. Never sends, deletes, moves or marks anything read.
// Degrades to a no-op if the package or the credentials are missing, so a
// problem here can never take the rest of the server down.
let ImapFlow = null;
try { ImapFlow = require('imapflow').ImapFlow; }
catch (e) { console.log('[Replies] imapflow not installed - reply watching is OFF'); }

const REPLY_ON       = () => String(process.env.REPLY_WATCH || 'true').toLowerCase() !== 'false';
const REPLY_USER     = () => process.env.GMAIL_USER || '';
const REPLY_PASS     = () => process.env.GMAIL_PASS || '';
const REPLY_LOOKBACK = () => parseInt(process.env.REPLY_LOOKBACK_DAYS || '3', 10);
const replyArmed     = () => !!(ImapFlow && REPLY_ON() && REPLY_USER() && REPLY_PASS());

let replyReady = false;
async function replyEnsure() {
  if (replyReady) return;
  await pool.query(`CREATE TABLE IF NOT EXISTS outreach_replies (
    id SERIAL PRIMARY KEY,
    contact_id BIGINT, from_email TEXT NOT NULL, subject TEXT,
    received_at TIMESTAMPTZ, detected_at TIMESTAMPTZ DEFAULT NOW(),
    message_uid TEXT UNIQUE, matched BOOLEAN DEFAULT false)`);
  await pool.query(`CREATE TABLE IF NOT EXISTS outreach_reply_state (
    k TEXT PRIMARY KEY, v TEXT, updated_at TIMESTAMPTZ DEFAULT NOW())`);
  replyReady = true;
}
async function replyGet(k, d) {
  await replyEnsure();
  const r = await pool.query('SELECT v FROM outreach_reply_state WHERE k=$1', [k]);
  return r.rows.length ? r.rows[0].v : d;
}
async function replySet(k, v) {
  await replyEnsure();
  await pool.query(`INSERT INTO outreach_reply_state (k,v,updated_at) VALUES ($1,$2,NOW())
                    ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`, [k, String(v)]);
}
function replyAddrOf(h) {
  // "Kurt London <kurt@firm.com>" -> kurt@firm.com
  const s = String(h && h.address ? h.address : h || '');
  const m = s.match(/[\w.+-]+@[\w-]+\.[\w.-]+/);
  return m ? m[0].toLowerCase() : '';
}

async function scanReplies() {
  if (!replyArmed()) {
    return { armed: false, reason: !ImapFlow ? 'imapflow not installed'
      : !REPLY_ON() ? 'REPLY_WATCH is false'
      : 'GMAIL_USER / GMAIL_PASS not set' };
  }
  await replyEnsure();
  // who are we listening for
  await orEnsureTable();
  const contacts = (await pool.query('SELECT id, data FROM outreach_contacts')).rows
    .map(r => Object.assign({}, r.data, { id: Number(r.id) }));
  const byEmail = {};
  for (const c of contacts) {
    const v = String(c.contact || '').trim();
    if (v.indexOf('@') > 0) byEmail[v.toLowerCase()] = c;
  }
  if (!Object.keys(byEmail).length) return { armed: true, checked: 0, matched: 0, note: 'no contacts with an email' };

  const sinceIso = await replyGet('last_scan_at', null);
  const since = sinceIso ? new Date(sinceIso) : new Date(Date.now() - REPLY_LOOKBACK() * 86400000);

  const client = new ImapFlow({
    host: 'imap.gmail.com', port: 993, secure: true,
    auth: { user: REPLY_USER(), pass: REPLY_PASS() },
    logger: false, emitLogs: false
  });
  const found = [];
  let checked = 0;
  try {
    await client.connect();
    const lock = await client.getMailboxLock('INBOX');          // read-only use
    try {
      for await (const msg of client.fetch({ since }, { envelope: true, uid: true })) {
        checked++;
        const env = msg.envelope || {};
        const from = replyAddrOf((env.from && env.from[0]) || '');
        if (!from) continue;
        const c = byEmail[from];
        if (!c) continue;
        const uid = String(msg.uid) + '@' + (env.date ? new Date(env.date).getTime() : '0');
        const ins = await pool.query(
          `INSERT INTO outreach_replies (contact_id, from_email, subject, received_at, message_uid, matched)
           VALUES ($1,$2,$3,$4,$5,true) ON CONFLICT (message_uid) DO NOTHING RETURNING id`,
          [c.id, from, String(env.subject || '').slice(0, 300), env.date || null, uid]);
        if (!ins.rows.length) continue;                          // already handled
        found.push({ contact_id: c.id, name: c.name, email: from, subject: env.subject || '' });
      }
    } finally { lock.release(); }
  } catch (e) {
    console.error('[Replies] scan failed:', e.message);
    await replySet('last_error', e.message.slice(0, 300));
    return { armed: true, error: e.message.slice(0, 200) };
  } finally {
    try { await client.logout(); } catch (e) {}
  }

  // mark them on the board and stop their sequences
  for (const f of found) {
    await pool.query(
      `UPDATE outreach_contacts SET data = data || jsonb_build_object(
         'emailStatus','replied',
         'lastContact', to_char((NOW() AT TIME ZONE 'America/New_York')::date,'YYYY-MM-DD'),
         'stage', CASE WHEN COALESCE(data->>'stage','') IN ('cold','contacted') THEN 'responded'
                       ELSE COALESCE(data->>'stage','responded') END),
         updated_at = NOW()
       WHERE id=$1`, [f.contact_id]);
    try {
      await pool.query(`UPDATE outreach_sequence_state SET status='stopped', stop_reason='they replied', updated_at=NOW()
                         WHERE contact_id=$1 AND status='active'`, [f.contact_id]);
      await pool.query(`UPDATE outreach_drafts SET status='skipped', error='they replied'
                         WHERE contact_id=$1 AND status IN ('pending','approved')`, [f.contact_id]);
    } catch (e) { /* sequence tables only exist once patch 287 is deployed */ }
  }

  await replySet('last_scan_at', new Date().toISOString());
  await replySet('last_error', '');

  if (found.length) {
    const rows = found.map(f =>
      '<tr><td style="padding:4px 10px"><b>' + String(f.name || '').replace(/</g, '&lt;') + '</b></td>' +
      '<td style="padding:4px 10px">' + f.email + '</td>' +
      '<td style="padding:4px 10px">' + String(f.subject).slice(0, 80).replace(/</g, '&lt;') + '</td></tr>').join('');
    await sendEmailNotification(
      found.length + ' outreach repl' + (found.length === 1 ? 'y' : 'ies'),
      '<p>' + found.length + ' contact' + (found.length === 1 ? '' : 's') +
      ' replied. Their sequences are stopped and they are marked Responded on the board.</p>' +
      '<table style="border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px">' + rows + '</table>');
  }
  return { armed: true, checked, matched: found.length, replies: found };
}

app.post('/outreach/replies/scan', async (req, res) => {
  if (!orAuth(req, res)) return;
  try { res.json(Object.assign({ ok: true }, await scanReplies())); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/outreach/replies/status', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await replyEnsure();
    const n = await pool.query('SELECT COUNT(*)::int c FROM outreach_replies WHERE matched');
    res.json({ ok: true,
      armed: replyArmed(),
      watching: REPLY_USER() || null,
      imapflow_installed: !!ImapFlow,
      last_scan_at: await replyGet('last_scan_at', null),
      last_error: (await replyGet('last_error', '')) || null,
      replies_detected_total: n.rows[0].c });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/outreach/replies/log', async (req, res) => {
  if (!orAuth(req, res)) return;
  try {
    await replyEnsure();
    const r = await pool.query(`SELECT * FROM outreach_replies ORDER BY detected_at DESC LIMIT 100`);
    res.json({ ok: true, replies: r.rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

setInterval(() => {
  if (!replyArmed()) return;
  scanReplies().then(r => { if (r && r.matched) console.log('[Replies] matched', r.matched); })
               .catch(e => console.error('[Replies] poll failed:', e.message));
}, 15 * 60 * 1000);

console.log('[Replies] reply watcher ' + (replyArmed()
  ? 'ARMED on ' + REPLY_USER() + ' (every 15 min, read-only)'
  : 'OFF - ' + (!ImapFlow ? 'imapflow not installed' : !REPLY_ON() ? 'REPLY_WATCH=false' : 'GMAIL_USER/GMAIL_PASS not set')));
// ─── end patch 291 ────────────────────────────────────────────────────────────
'''

src = src.replace(anchor, anchor + "\n" + NEW, 1)

# add the dependency
pkg_path = os.path.join(os.path.dirname(TARGET), "package.json")
pkg_note = "package.json not found - add imapflow yourself"
if os.path.exists(pkg_path):
    pkg = json.load(open(pkg_path, encoding="utf-8"))
    deps = pkg.setdefault("dependencies", {})
    if "imapflow" in deps:
        pkg_note = "imapflow already in package.json"
    else:
        deps["imapflow"] = "^1.0.164"
        shutil.copy2(pkg_path, pkg_path + ".pre-291.bak")
        json.dump(pkg, open(pkg_path, "w", encoding="utf-8"), indent=2)
        open(pkg_path, "a", encoding="utf-8").write("\n")
        pkg_note = "imapflow ^1.0.164 added to package.json"

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-291.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 291 applied. backup: server.js.pre-291.bak")
print("  " + pkg_note)
print("  the watcher stays OFF until imapflow installs and GMAIL_USER/GMAIL_PASS are set")
print("next: node --check server.js && git add -A && git commit -m 'patch 291' && git push")
