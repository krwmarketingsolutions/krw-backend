#!/usr/bin/env python3
"""
patch-298.py  --  NLD is rejecting leads on incident_date FORMAT, not quality.

THE BUG (live evidence, Oct 7)
  Three Leadbloom leads were offered to NLD CPA and all three came back:
      Error: field `incident_date` has wrong format
  NLD campaign 31080 wants mm/dd/yyyy. The code deliberately passes
  incident_date through unconverted (see the comment above convertDateToISO's
  use in MVA_BUYERS) because every publisher that existed when that was written
  sends mm/dd/yyyy.

  Leadbloom does not. Verified across 739 leads:
      KRW-NYC-MVA        452 leads   100% mm/dd/yyyy
      KRW-INBOUNDS-CPL   173 leads   100% mm/dd/yyyy
      KRW-KANTHONY-RS     55 leads       mm/dd/yyyy (one 06/19/25)
      KRW-LEADBLOOM-MVA    8 leads   ISO yyyy-mm-dd, plus one full timestamp
  And of 43 leads NLD has ever ACCEPTED, 43/43 carried mm/dd/yyyy. Zero ISO.

  So every Leadbloom lead in NLD's 10 states fails the post outright and falls
  to MVA-003-LT at $1,700 instead of NLD at $2,000. Worse, 003 is capped at
  5/day, so once it fills the lead goes nowhere at all - that is exactly how
  Steven Bentley (2330) ended up undelivered with a valid cert.

WHAT THIS CHANGES
  Adds toUsDate() and uses it on incident_date in all THREE places that post to
  NLD campaign 31080:
      1. MVA_BUYERS 'NLD CPA'.post      (the generic MVA router)
      2. the MVA-NYC-SPLIT buyer ladder (what Leadbloom rides)
      3. janSend()                      (the 6am janitor's overnight retry)
  Formats handled: yyyy-mm-dd, full ISO timestamps, mm-dd-yyyy, and 2-digit
  years. Anything unrecognized passes through untouched rather than being
  mangled.

  Nothing else is touched. CH-Intake, LT-Intake and MVA-003-LT keep receiving
  incident_date exactly as the publisher sent it - 003 accepted ISO fine today,
  so there is no reason to change what already works.

Usage (from ~/krw/krw-backend):
    python3 patch-298.py
    node --check server.js
    git add -A && git commit -m "patch 298: send NLD incident_date as mm/dd/yyyy" && git push

Backup: server.js.pre-298.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 298" in src:
    sys.exit("patch 298 already applied - nothing to do")

# ── 1. the helper, dropped in right after convertDateToISO ───────────────────
ANCHOR = """  return dateStr; // unrecognized format, pass through unchanged rather than silently drop it
}
"""
if src.count(ANCHOR) != 1:
    sys.exit("could not find the end of convertDateToISO - aborting, server.js untouched")

HELPER = ANCHOR + """
// patch 298: NLD campaign 31080 requires incident_date as mm/dd/yyyy and
// rejects the whole post with "field `incident_date` has wrong format"
// otherwise. Most publishers already send mm/dd/yyyy; Leadbloom sends ISO,
// which is why 100% of its leads were failing to NLD and dropping to 003 at
// $300 less. This normalizes on the way out so the publisher's format no
// longer decides which buyer a lead can reach.
function toUsDate(v) {
  if (!v) return v;
  const s = String(v).trim();
  if (!s) return v;
  let m = s.match(/^(\\d{4})-(\\d{1,2})-(\\d{1,2})(?:[T ].*)?$/);      // ISO date, or a full timestamp
  if (m) return m[2].padStart(2, '0') + '/' + m[3].padStart(2, '0') + '/' + m[1];
  m = s.match(/^(\\d{1,2})[\\/\\-](\\d{1,2})[\\/\\-](\\d{4})$/);          // mm/dd/yyyy or mm-dd-yyyy
  if (m) return m[1].padStart(2, '0') + '/' + m[2].padStart(2, '0') + '/' + m[3];
  m = s.match(/^(\\d{1,2})[\\/\\-](\\d{1,2})[\\/\\-](\\d{2})$/);          // 2-digit year, eg 06/19/25
  if (m) return m[1].padStart(2, '0') + '/' + m[2].padStart(2, '0') + '/20' + m[3];
  return v;                        // unrecognized - pass through, do not mangle
}
"""
src = src.replace(ANCHOR, HELPER, 1)

# ── 2. the three NLD senders ─────────────────────────────────────────────────
EDITS = [
    ("MVA_BUYERS 'NLD CPA'.post",
     """        incident_state: incidentStateFull,
        incident_date:  b.incident_date,""",
     """        incident_state: incidentStateFull,
        incident_date:  toUsDate(b.incident_date),   // patch 298"""),

    ("MVA-NYC-SPLIT buyer ladder",
     "        incident_state: incidentStateFull, incident_date: b.incident_date,",
     "        incident_state: incidentStateFull, incident_date: toUsDate(b.incident_date),   // patch 298"),

    ("janSend() overnight retry",
     "      jornaya_leadid: b.jornaya_leadid || undefined, incident_state: full, incident_date: b.incident_date,",
     "      jornaya_leadid: b.jornaya_leadid || undefined, incident_state: full, incident_date: toUsDate(b.incident_date),   // patch 298"),
]

for label, old, new in EDITS:
    n = src.count(old)
    if n != 1:
        sys.exit("%s: expected exactly 1 match, found %d - aborting, server.js untouched" % (label, n))
    src = src.replace(old, new, 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-298.bak"))
open(TARGET, "w", encoding="utf-8").write(src)

print("patch 298 applied. backup: server.js.pre-298.bak")
for label, _, _ in EDITS:
    print("  incident_date -> mm/dd/yyyy in %s" % label)
print("")
print("sanity check of the converter:")
print("  2025-11-03                 -> 11/03/2025")
print("  2026-10-05T00:00:00+0000   -> 10/05/2026")
print("  06/19/25                   -> 06/19/2025")
print("  11/3/2025                  -> 11/03/2025")
print("  (anything else)            -> unchanged")
print("")
print("next: node --check server.js && git add -A && git commit -m 'patch 298' && git push")
