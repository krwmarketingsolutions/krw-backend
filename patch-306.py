#!/usr/bin/env python3
"""
patch-306.py  --  Stop the Laird sheet poller (dead line, broken fetch)

WHAT WAS FOUND (Oct 7)
  The hourly Laird poller fetches the sheet as an anonymous CSV export:
      docs.google.com/spreadsheets/d/1SJi0U.../export?format=csv&gid=0
  but the sheet ("TRUTH reporting", owned by spadam8802@gmail.com) is
  private - shared person-to-person only (kyler@leadbloom.co is a writer;
  no "anyone with link", no service account). Anonymous export therefore
  returns a Google LOGIN PAGE, and the poller has been parsing that HTML
  every hour and logging "could not find header row" - twice per run,
  since both its Roblox and Rideshare entries point at the same gid=0.

  And the line itself is dead: 22 Laird leads ever, the newest Jun 22;
  the sheet last edited Jul 20.

WHAT THIS DOES
  The hourly schedule no longer starts unless LAIRD_POLL=true is set on
  Railway. The code, the parser and the manual trigger
  (GET /debug-poll-laird) all stay, so if Laird comes back the poller is
  one env var away - but to actually WORK it will also need the sheet
  shared properly (link-view, or the service account + an authed fetch,
  which would be its own patch).

Usage (from ~/krw/krw-backend):
    python3 patch-306.py
    node --check server.js
    git add -A && git commit -m "patch 306: Laird poller off by default" && git push

Backup: server.js.pre-306.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 306" in src:
    sys.exit("patch 306 already applied - nothing to do")

OLD = """// Start polling 30 seconds after boot (offset from KA poller), then every hour
setTimeout(() => {
  pollLairdLeadsSheet();
  setInterval(pollLairdLeadsSheet, LAIRD_POLL_INTERVAL_MS);
}, 30000);"""

if src.count(OLD) != 1:
    sys.exit("the Laird poller startup does not look the way patch 306 expects - aborting, server.js untouched")

NEW = """// patch 306: poller OFF by default. The sheet is private (anonymous CSV
// export returns a Google login page, which the parser then chewed on every
// hour) and the Laird line has been inactive since June. Set LAIRD_POLL=true
// on Railway to re-arm - and share the sheet with link-view or the service
// account first, or it will just fail politely again.
if (process.env.LAIRD_POLL === 'true') {
  setTimeout(() => {
    pollLairdLeadsSheet();
    setInterval(pollLairdLeadsSheet, LAIRD_POLL_INTERVAL_MS);
  }, 30000);
  console.log('[Laird Sheet Poll] armed - hourly (LAIRD_POLL=true)');
} else {
  console.log('[Laird Sheet Poll] OFF (patch 306) - set LAIRD_POLL=true on Railway to re-arm; manual: GET /debug-poll-laird');
}"""

src = src.replace(OLD, NEW, 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-306.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 306 applied. backup: server.js.pre-306.bak")
print("  Laird hourly poller off by default; LAIRD_POLL=true re-arms it")
print("  manual trigger GET /debug-poll-laird still available")
