#!/usr/bin/env python3
r"""
patch-357.py  --  derive lead channel from the landing page URL

The channel was taken from the form's posted value, which is hardcoded to
"Facebook", so a Google-sourced lead (utm_source=google in the landing page
URL) still reported as Facebook. This reads the landing page URL instead:
contains "facebook"/fbclid -> Facebook, "google"/gclid -> Google, otherwise
"Landing page". Applied at ingest so all reporting is accurate going forward.

  krw-backend/server.js
    - new helper chanFromLanding(url)
    - set b.channel from it in mva-nyc-split and mva-funnel (covers Leadbloom,
      Kevin and NYC, since Leadbloom/Kevin reroute into nyc-split)
    - mva-email-agency: both channel fields use the helper too

Backup: server.js.pre-357.bak    Safe to re-run: refuses twice.
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
if "patch 357" in src or "function chanFromLanding" in src:
    sys.exit("patch 357 already applied - nothing to do")

edits = []

# E1 - helper (placed before tfValid; function declarations hoist)
edits.append((
"function tfValid(u) { return typeof u === 'string' && TF_CERT_RE.test(u.trim()); }",
"// patch 357: channel reflects where the lead actually came from, read off the landing page URL\n"
"function chanFromLanding(url) {\n"
"  const u = String(url || '').toLowerCase();\n"
"  if (u.indexOf('facebook') >= 0 || u.indexOf('fbclid') >= 0) return 'Facebook';\n"
"  if (u.indexOf('google') >= 0 || u.indexOf('gclid') >= 0) return 'Google';\n"
"  return 'Landing page';\n"
"}\n"
"function tfValid(u) { return typeof u === 'string' && TF_CERT_RE.test(u.trim()); }"))

# E2 - mva-nyc-split (handles Leadbloom, Kevin, NYC)
edits.append((
"  const b = req.body || {};\n"
"  const leadState = (b.state || b.incident_state || '').toUpperCase().trim();",
"  const b = req.body || {};\n"
"  const leadState = (b.state || b.incident_state || '').toUpperCase().trim();\n"
"  b.channel = chanFromLanding(b.landing_page_url);   // patch 357"))

# E3 - mva-funnel (before it reroutes Leadbloom/Kevin into nyc-split)
edits.append((
"  const b = req.body || {};\n"
"\n"
"  // patch 266: Kevin Anthony's MVA traffic rides the NYC buyer ladder",
"  const b = req.body || {};\n"
"  b.channel = chanFromLanding(b.landing_page_url);   // patch 357\n"
"\n"
"  // patch 266: Kevin Anthony's MVA traffic rides the NYC buyer ladder"))

# E4 - mva-email-agency channel #1
edits.append((
"      channel:               (['Facebook','Google','Email','SMS','Display','Native','Other'].includes(b.channel) ? b.channel : 'Facebook'),",
"      channel:               chanFromLanding(b.landing_page_url),   // patch 357"))

# E5 - mva-email-agency channel #2
edits.append((
"        channel:      (['Facebook','Google','Email','SMS','Display','Native','Other'].includes(b.channel) ? b.channel : 'Facebook'),",
"        channel:      chanFromLanding(b.landing_page_url),   // patch 357"))

for i, (a, r) in enumerate(edits, 1):
    n = src.count(a)
    if n != 1:
        sys.exit("ABORT: edit E%d anchor found %d times, expected 1. No change." % (i, n))

for a, r in edits:
    src = src.replace(a, r, 1)

shutil.copyfile(FILE, FILE + ".pre-357.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 357 applied to", FILE)
