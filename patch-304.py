#!/usr/bin/env python3
"""
patch-304.py  --  Two emails a day, not thirty: the KRW brief

WHAT WAS FLOODING THE INBOX (Kyler, Oct 7: "I'm getting an email every 20
minutes")
  Two sweeps run every 30 minutes - the alert sweep (cap burns, TrustedForm
  spikes, silent sheets, scanner failures) and the chase list. Both are
  supposed to email once per day, but their "already sent today" memory is
  IN-MEMORY, and the comment even says "a redeploy may re-send at most one
  round - harmless". Today the backend deployed 7+ times (patches 295-303),
  and every single boot re-sent the same alerts 3-4 minutes later. The
  repeating one: "Consent alert - KRW-LEADBLOOM-MVA hit the TrustedForm gate
  3x today".

WHAT THIS DOES
  1. notify_outbox table: alert dedupe moves to the database with a UNIQUE
     constraint, so redeploys can never re-send anything again, ever.
  2. Alerts no longer email at all when detected - they QUEUE, and go out
     inside a twice-daily brief:
         8:00 AM Pacific  - morning brief
         7:00 PM Pacific  - evening brief (includes the chase list)
     Each brief also opens with the day so far: leads in, delivered,
     rejected, billable revenue, approval-queue count. Override times with
     BRIEF_TIMES_PT on Railway (e.g. "07:30,19:00").
  3. The chase list's own 30-minute schedule is removed - it becomes a
     section of the evening brief.
  4. The brief send itself is claimed through the same table, so even a
     deploy AT 7:00 PM cannot double-send.

  Left alone on purpose (rare, and they need action): new-billable approval
  emails, signed-case sheet finds, outreach replies/drafts, the 6am janitor
  summary, weekly reports, the SSDI daily report.

Usage (from ~/krw/krw-backend):
    python3 patch-304.py
    node --check server.js
    git add -A && git commit -m "patch 304: twice-daily brief" && git push

Backup: server.js.pre-304.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 304" in src:
    sys.exit("patch 304 already applied - nothing to do")

def must_replace(label, old, new):
    global src
    n = src.count(old)
    if n != 1:
        sys.exit("%s: expected exactly 1 match, found %d - aborting, server.js untouched" % (label, n))
    src = src.replace(old, new, 1)

# ── 1. DB-backed queue replaces the in-memory dedupe ─────────────────────────
must_replace("kaFired -> kaQueue",
"""const kylerAlertsSent = new Map();
function kaFired(etDate, type, key) {
  const k = etDate + '|' + type + '|' + key;
  if (kylerAlertsSent.has(k)) return true;
  if (kylerAlertsSent.size > 1000) kylerAlertsSent.clear();
  kylerAlertsSent.set(k, true);
  return false;
}""",
"""// patch 304: dedupe and delivery live in the database now. Alerts queue in
// notify_outbox and go out inside the twice-daily brief instead of emailing
// the moment they are detected. The UNIQUE constraint is what makes the
// dedupe survive redeploys - the in-memory Map this replaces re-sent the
// same alerts after every one of today's 7 deploys.
async function notifyOutboxInit() {
  await pool.query(`CREATE TABLE IF NOT EXISTS notify_outbox (
    id SERIAL PRIMARY KEY,
    day TEXT NOT NULL,
    type TEXT NOT NULL,
    key TEXT NOT NULL DEFAULT '',
    subject TEXT,
    html TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    UNIQUE (day, type, key)
  )`);
}
notifyOutboxInit().catch(e => console.error('[Notify Outbox] init error:', e.message));
async function kaQueue(etDate, type, key, subject, html) {
  try {
    await pool.query(
      `INSERT INTO notify_outbox (day, type, key, subject, html)
       VALUES ($1,$2,$3,$4,$5) ON CONFLICT (day, type, key) DO NOTHING`,
      [etDate, type, String(key || ''), subject, html]);
  } catch (e) { console.error('[Notify Outbox] queue error:', e.message); }
}""")

# ── 2. the four alert senders queue instead of emailing ─────────────────────
must_replace("cap burn alert",
"""    for (const r of cap.rows) {
      if (kaFired(etDate, 'capburn', r.publisher_sub)) continue;
      await sendEmailNotification(
        `⚠ Cap burn — ${r.publisher_sub}: ${r.n} leads rejected on volume caps today`,
        `<p><b>${r.n}</b> leads from <b>${r.publisher_sub}</b> hit daily volume caps today (ET) and earned nothing. Consider throttling the publisher or revisiting the cap.</p>`);
    }""",
"""    for (const r of cap.rows) {
      await kaQueue(etDate, 'capburn', r.publisher_sub,
        `Cap burn — ${r.publisher_sub}: ${r.n} leads rejected on volume caps today`,
        `<p><b>${r.n}</b> leads from <b>${r.publisher_sub}</b> hit daily volume caps today (ET) and earned nothing. Consider throttling the publisher or revisiting the cap.</p>`);
    }""")

must_replace("tf gate alert",
"""    for (const r of tf.rows) {
      if (kaFired(etDate, 'tfgate', r.publisher_sub)) continue;
      await sendEmailNotification(
        `⚠ Consent alert — ${r.publisher_sub} hit the TrustedForm gate ${r.n}x today`,
        `<p><b>${r.publisher_sub}</b> sent <b>${r.n}</b> leads today that were rejected for missing, invalid or REUSED TrustedForm certificates. Reused certs mean recycled consent — worth a direct conversation with the publisher.</p>`);
    }""",
"""    for (const r of tf.rows) {
      await kaQueue(etDate, 'tfgate', r.publisher_sub,
        `Consent alert — ${r.publisher_sub} hit the TrustedForm gate ${r.n}x today`,
        `<p><b>${r.publisher_sub}</b> sent <b>${r.n}</b> leads today that were rejected for missing, invalid or REUSED TrustedForm certificates. Reused certs mean recycled consent — worth a direct conversation with the publisher.</p>`);
    }""")

must_replace("stale sheet alert",
"""        if (hrs >= 48 && !kaFired(etDate, 'stalesheet', r.buyer_key)) {
          await sendEmailNotification(
            `⚠ Buyer sheet silent — ${r.buyer_key} not updated in ${hrs}h`,
            `<p>The <b>${r.buyer_key}</b> disposition sheet was last edited <b>${hrs} hours ago</b>. Leads are likely sitting unworked or undispositioned — worth a nudge.</p>`);
        }""",
"""        if (hrs >= 48) {
          await kaQueue(etDate, 'stalesheet', r.buyer_key,
            `Buyer sheet silent — ${r.buyer_key} not updated in ${hrs}h`,
            `<p>The <b>${r.buyer_key}</b> disposition sheet was last edited <b>${hrs} hours ago</b>. Leads are likely sitting unworked or undispositioned — worth a nudge.</p>`);
        }""")

must_replace("scan fail alert",
"""      for (const r of fails.rows) {
        if (kaFired(etDate, 'scanfail', r.buyer_key)) continue;
        await sendEmailNotification(
          `⚠ Sheet scanner failing — ${r.buyer_key}`,
          `<p>The last two scans of the <b>${r.buyer_key}</b> sheet both failed. Latest error: <code>${String(r.last_error || 'unknown').slice(0, 200)}</code>. Dispositions are not syncing until this is fixed.</p>`);
      }""",
"""      for (const r of fails.rows) {
        await kaQueue(etDate, 'scanfail', r.buyer_key,
          `Sheet scanner failing — ${r.buyer_key}`,
          `<p>The last two scans of the <b>${r.buyer_key}</b> sheet both failed. Latest error: <code>${String(r.last_error || 'unknown').slice(0, 200)}</code>. Dispositions are not syncing until this is fixed.</p>`);
      }""")

must_replace("alert sweep log line",
"""console.log('[Kyler Alerts] sweep armed - every 30 min, email to', process.env.NOTIFY_EMAIL || '(NOTIFY_EMAIL unset)');""",
"""console.log('[Kyler Alerts] sweep armed - every 30 min, QUEUES into the twice-daily brief (patch 304)');""")

# ── 3. chase list becomes a section builder ──────────────────────────────────
must_replace("chase fn header",
"""let chaseLastSent = null;
async function staleChaseSweep() {""",
"""async function buildChaseSection() {   // patch 304: returns html for the evening brief instead of emailing""")

must_replace("chase gates",
"""    if (etDow < 1 || etDow > 5 || etHour < 9 || etHour > 20) return;
    if (chaseLastSent === etDate) return;
""", "")

must_replace("chase empty return",
"""    if (!r.rows.length) { chaseLastSent = etDate; return; }""",
"""    if (!r.rows.length) return '';""")

must_replace("chase tail",
"""    await sendEmailNotification(`⚠ Chase list — ${r.rows.length} lead${r.rows.length > 1 ? 's' : ''} waiting on buyer updates`, html);
    chaseLastSent = etDate;
    console.log(`[Chase List] sent - ${noDispo.length} undispositioned, ${stuck.length} stuck`);
  } catch (e) { console.error('[Chase List] sweep error:', e.message); }
}
setInterval(staleChaseSweep, 30 * 60 * 1000);
setTimeout(staleChaseSweep, 4 * 60 * 1000);
console.log('[Chase List] armed - daily digest of leads waiting on buyer updates');""",
"""    return `<h3>Chase list — ${r.rows.length} lead${r.rows.length > 1 ? 's' : ''} waiting on buyer updates</h3>` + html;
  } catch (e) { console.error('[Chase List] build error:', e.message); return ''; }
}
console.log('[Chase List] folded into the evening brief (patch 304)');

// ─── patch 304: the twice-daily brief ────────────────────────────────────────
// The ONLY scheduled notification emails: 8:00 AM and 7:00 PM Pacific
// (override with BRIEF_TIMES_PT, comma-separated HH:MM). Everything the
// alert sweep finds in between queues silently and ships inside these.
const BRIEF_TIMES_PT = (process.env.BRIEF_TIMES_PT || '08:00,19:00').split(',').map(s => s.trim());
async function sendDailyBrief(slot) {
  // claim the slot in the DB first - a redeploy at exactly send time, or a
  // second instance, can never double-send
  const day = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
  const claim = await pool.query(
    `INSERT INTO notify_outbox (day, type, key, subject) VALUES ($1,'brief',$2,'claimed')
     ON CONFLICT (day, type, key) DO NOTHING RETURNING id`, [day, slot]);
  if (!claim.rows.length) return;

  const s = await pool.query(
    `SELECT COUNT(*)::int AS total,
            COUNT(*) FILTER (WHERE status='forwarded')::int AS delivered,
            COUNT(*) FILTER (WHERE status IN ('rejected','buyer_rejected'))::int AS rejected,
            COALESCE(SUM(revenue) FILTER (WHERE billable), 0)::numeric AS revenue
     FROM leads
     WHERE COALESCE(raw->>'excluded','') <> 'true' AND COALESCE(vertical,'') <> 'SSDI'
       AND (received_at AT TIME ZONE 'America/Los_Angeles')::date = (NOW() AT TIME ZONE 'America/Los_Angeles')::date`);
  const q = await pool.query(`SELECT COUNT(*)::int AS n FROM billable_queue WHERE status='pending'`);
  const alerts = await pool.query(
    `SELECT id, subject, html FROM notify_outbox
     WHERE sent_at IS NULL AND type <> 'brief' AND subject IS NOT NULL AND html IS NOT NULL
     ORDER BY created_at ASC LIMIT 40`);
  const chase = slot === 'pm' ? await buildChaseSection() : '';
  const t = s.rows[0];
  let html = `<p><b>Today so far (PT):</b> ${t.total} MVA lead${t.total === 1 ? '' : 's'} in, ` +
             `${t.delivered} delivered, ${t.rejected} rejected, $${parseFloat(t.revenue).toFixed(0)} billable revenue. ` +
             `${q.rows[0].n} item${q.rows[0].n === 1 ? '' : 's'} in the approval queue.</p>`;
  if (alerts.rows.length) {
    html += '<h3>Alerts since the last brief</h3>' +
      alerts.rows.map(a => `<p><b>${a.subject}</b></p>${a.html}`).join('');
  }
  if (chase) html += chase;
  if (!alerts.rows.length && !chase) html += '<p>No alerts. Nothing is waiting on you beyond the approval queue.</p>';
  const label = slot === 'am' ? 'Morning' : 'Evening';
  await sendEmailNotification(`KRW brief — ${label}`, html);
  if (alerts.rows.length) {
    await pool.query(`UPDATE notify_outbox SET sent_at = NOW() WHERE id = ANY($1::int[])`, [alerts.rows.map(a => a.id)]);
  }
  console.log(`[Daily Brief] ${label} sent - ${alerts.rows.length} alert(s)` + (chase ? ' + chase list' : ''));
}
setInterval(() => {
  const hm = new Date().toLocaleTimeString('en-GB', { timeZone: 'America/Los_Angeles', hour: '2-digit', minute: '2-digit' });
  const idx = BRIEF_TIMES_PT.indexOf(hm);
  if (idx < 0) return;
  sendDailyBrief(idx === 0 ? 'am' : 'pm').catch(e => console.error('[Daily Brief] error:', e.message));
}, 60 * 1000);
console.log('[Daily Brief] armed - Pacific times:', BRIEF_TIMES_PT.join(', '));
// ─── end patch 304 ────────────────────────────────────────────────────────────""")

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-304.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 304 applied. backup: server.js.pre-304.bak")
print("  alerts + chase list -> queue in notify_outbox (DB dedupe, deploy-proof)")
print("  one brief at 8:00 AM PT, one at 7:00 PM PT (BRIEF_TIMES_PT overrides)")
print("  untouched: approval emails, janitor summary, weekly reports, outreach replies")
