#!/usr/bin/env python3
r"""
patch-370.py  --  fix CAG accept/reject parsing (CAG returns HTTP 200 on reject)

Verified against CAG's live endpoint with two test posts (Nov 10):
  * valid lead  -> HTTP 200, body "Lead Accepted"
  * bad state   -> HTTP 200, body 'state does not have a valid value "ZZ".'

CAG returns 200 for BOTH outcomes, so HTTP status is NOT a signal - the body is.
The patch 369 parser accepted on any 2xx without a rejection keyword, which
wrongly marks validation/dupe rejects (no "accept" word in them) as accepted.
Fix: a CAG post is accepted ONLY when the body actually says "accept"
("Lead Accepted"); everything else (field-validation messages, duplicates, etc.)
is a rejection, with the message captured.

  krw-backend/server.js (one edit, inside sendToCag).

Backup: server.js.pre-370.bak    Safe to re-run: refuses twice.
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
if "patch 370" in src:
    sys.exit("patch 370 already applied - nothing to do")

anchor = (
"  const looksRejected = /reject|declin|duplicat|\\bdupe\\b|invalid|\\berror\\b|\\bfail|not\\s*accept/.test(low);\n"
"  const looksAccepted = /accept|success|received|\"?status\"?\\s*[:=]\\s*\"?(1|accepted|success)/.test(low);\n"
"  // Until CAG's real response is seen: accept on 2xx with no rejection signal.\n"
"  const accepted = http2xx && !looksRejected && (looksAccepted || true);")

repl = (
"  // patch 370: CAG returns HTTP 200 for BOTH accept and reject, so the body is\n"
"  // the only signal. Accept body is \"Lead Accepted\"; rejects are validation /\n"
"  // dupe messages with no \"accept\" in them (e.g. 'state does not have a valid\n"
"  // value \"ZZ\".'). Accept ONLY on an explicit accept word.\n"
"  const accepted = http2xx && /accept/i.test(bodyText) && !/not\\s+accept|un\\s*accept/i.test(low);")

if src.count(anchor) != 1:
    sys.exit("ABORT: anchor found %d times, expected 1. No change." % src.count(anchor))
src = src.replace(anchor, repl, 1)
shutil.copyfile(FILE, FILE + ".pre-370.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 370 applied to", FILE)
