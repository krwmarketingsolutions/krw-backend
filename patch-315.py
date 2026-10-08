#!/usr/bin/env python3
"""
patch-315.py  --  Template store learns the intro A/B variants (pairs with 316)

Kyler wants the first-touch email A/B tested: variant A and variant D rotate
per contact and response rates decide the winner. The /outreach/templates
whitelist gains 'intro_a' and 'intro_d' so both variants live server side.
Everything else untouched.

Usage (from ~/krw/krw-backend):
    python3 patch-315.py && node --check server.js
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found")
src = open(TARGET, encoding="utf-8").read()
if "patch 315" in src:
    sys.exit("patch 315 already applied - nothing to do")

OLD = "for (const k of ['intro', 'follow', 'bump', 'li_follow']) {"
n = src.count(OLD)
if n != 2:
    sys.exit("expected the template key list exactly twice, found %d - aborting" % n)
src = src.replace(OLD, "for (const k of ['intro', 'intro_a', 'intro_d', 'follow', 'bump', 'li_follow']) {   // patch 315: A/B intro variants")

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-315.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 315 applied")
