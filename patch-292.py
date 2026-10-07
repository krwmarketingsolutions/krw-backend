#!/usr/bin/env python3
"""
patch-292.py  --  Fix the CORS header list so the dashboard login actually works

THE BUG (mine, introduced by patch 288)
  Patch 288 moved the dashboard onto a signed token sent as:
      Authorization: Bearer <token>
  but the server's CORS policy still only allowed:
      Access-Control-Allow-Headers: Content-Type, x-api-key
  A browser refuses to send a header the server has not allowed, so every
  dashboard request failed its preflight and the page showed
  "Error: Failed to fetch" with no data. Nothing was wrong with the login,
  the token, or the key - the browser never let the request leave.

WHAT IT CHANGES (three strings, nothing else)
  Access-Control-Allow-Headers : adds Authorization
  Access-Control-Allow-Methods : adds PUT and DELETE
      PUT was already missing, which would have broken saving the Outreach
      board (PUT /outreach/contacts) the same way.
  Access-Control-Max-Age       : added, so browsers cache the preflight for
      10 minutes instead of re-asking before every single call.

Usage (from ~/krw/krw-backend):
    python3 patch-292.py
    node --check server.js
    git add -A && git commit -m "patch 292: allow Authorization header in CORS" && git push

Backup: server.js.pre-292.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 292" in src:
    sys.exit("patch 292 already applied - nothing to do")

OLD = """  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');"""

if src.count(OLD) != 1:
    sys.exit("the CORS block does not look the way patch 292 expects - aborting, server.js untouched")

NEW = """  // patch 292: Authorization is what the dashboard's login token rides in.
  // Without it here the browser blocks the request before it is ever sent.
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Max-Age', '600');"""

src = src.replace(OLD, NEW, 1)

shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-292.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 292 applied. backup: server.js.pre-292.bak")
print("  Allow-Headers now includes Authorization")
print("  Allow-Methods now includes PUT and DELETE")
print("next: node --check server.js && git add -A && git commit -m 'patch 292' && git push")
