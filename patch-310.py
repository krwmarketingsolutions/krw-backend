#!/usr/bin/env python3
"""
patch-310.py  --  LinkedIn drafts land on the Outreach board automatically

WHY
  The "LinkedIn outreach drafts" agent emails Kyler a ready-to-send message
  whenever a lawyer accepts his connection request (subject
  "LinkedIn draft: <name> | <firm>", sent from waltersonkyle@gmail.com to
  kyler@leadbloom.co). Kyler wants every drafted person tracked on the
  dashboard's Outreach board, tagged LinkedIn, without doing it by hand.

HOW
  The reply watcher (patch 291) already holds IMAP credentials for the Gmail
  account the drafts are sent FROM. This patch adds a second scanner on the
  same 15 minute cycle: it opens [Gmail]/Sent Mail, finds messages whose
  subject starts with "LinkedIn draft:", and upserts each person onto the
  Outreach board:
    - new person: role buyer, vertical MVA, stage cold, source LinkedIn,
      notes "LinkedIn draft ready (emailed <date>)", firm from the subject
      when present
    - already on the board: ONLY stamps liDraftAt and fills source if empty.
      Stage, notes and everything Kyler may have edited are left alone.
  Idempotent via a kv watermark (li_drafts_last_scan) plus the upsert itself.

Usage (from ~/krw/krw-backend):
    python3 patch-310.py
    node --check server.js
    git add -A && git commit -m "patch 310: drafts -> outreach board" && git push

Backup: server.js.pre-310.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 310" in src:
    sys.exit("patch 310 already applied - nothing to do")

ANCHOR = """app.post('/outreach/replies/scan', async (req, res) => {"""
if src.count(ANCHOR) != 1:
    sys.exit("could not find the replies scan route - aborting, server.js untouched")

NEW = """// patch 310: track the LinkedIn draft agent's output on the Outreach board.
// The agent emails drafts FROM this same Gmail account, so they sit in
// [Gmail]/Sent Mail with subject "LinkedIn draft: <name> | <firm>". Each one
// becomes (or refreshes) a board contact tagged LinkedIn.
async function scanLinkedInDrafts() {
  if (!replyArmed()) return { armed: false };
  await replyEnsure();
  await orEnsureTable();
  const sinceIso = await replyGet('li_drafts_last_scan', null);
  const since = sinceIso ? new Date(sinceIso) : new Date(Date.now() - 7 * 86400000);
  const client = new ImapFlow({
    host: 'imap.gmail.com', port: 993, secure: true,
    auth: { user: REPLY_USER(), pass: REPLY_PASS() },
    logger: false, emitLogs: false
  });
  let added = 0, refreshed = 0;
  try {
    await client.connect();
    const lock = await client.getMailboxLock('[Gmail]/Sent Mail');
    try {
      for await (const msg of client.fetch({ since }, { envelope: true, uid: true })) {
        const subj = String((msg.envelope || {}).subject || '');
        if (subj.indexOf('LinkedIn draft:') !== 0) continue;
        const rest = subj.slice('LinkedIn draft:'.length).trim();
        const parts = rest.split('|').map(s => s.trim()).filter(Boolean);
        const name = parts[0] || '';
        const company = parts[1] || '';
        if (!name) continue;
        const day = new Date((msg.envelope && msg.envelope.date) || Date.now())
          .toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
        const existing = await pool.query(
          `SELECT id FROM outreach_contacts WHERE lower(data->>'name') = lower($1) LIMIT 1`, [name]);
        if (existing.rows.length) {
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
        }
      }
    } finally { lock.release(); }
    await replySet('li_drafts_last_scan', new Date(Date.now() - 3600000).toISOString());
    if (added || refreshed) console.log(`[LI Drafts] board sync: ${added} added, ${refreshed} refreshed`);
  } catch (e) {
    console.error('[LI Drafts] scan failed:', e.message);
  } finally {
    try { await client.logout(); } catch (e) {}
  }
  return { armed: true, added, refreshed };
}

""" + ANCHOR

src = src.replace(ANCHOR, NEW, 1)

OLD_TICK = """  scanReplies().then(r => { if (r && r.matched) console.log('[Replies] matched', r.matched); })
               .catch(e => console.error('[Replies] poll failed:', e.message));"""
if src.count(OLD_TICK) != 1:
    sys.exit("could not find the reply poll tick - aborting, server.js untouched")
src = src.replace(OLD_TICK, OLD_TICK + """
  scanLinkedInDrafts().catch(e => console.error('[LI Drafts] poll failed:', e.message));   // patch 310""", 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-310.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 310 applied. backup: server.js.pre-310.bak")
print("  every 'LinkedIn draft:' email the agent sends now lands on the Outreach board")
print("  existing contacts only get a liDraftAt stamp - nothing Kyler edited is touched")
