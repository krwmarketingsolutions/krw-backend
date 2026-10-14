#!/usr/bin/env python3
"""
patch-327.py  --  stop the dashboard logging out mid-task (12h -> 30 days)

The admin login token was hardcoded to a 12-hour life, so the dashboard
kicked you to the login screen twice a day. This extends it to 30 days.
Takes effect on the next login (the token's life is stamped when minted).

Trade-off: a longer-lived bearer token sits in the browser longer, so if
you ever think it leaked, change DASH_PASSWORD on Railway to invalidate
every existing token at once.

Backup: server.js.pre-327.bak    Safe to re-run: refuses twice.
"""
import os, shutil, sys
HERE=os.path.dirname(os.path.abspath(__file__))
def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c,"krw-backend")): return c
    return None
ROOT=find_root() or sys.exit("run from ~/krw")
FILE=os.path.join(ROOT,"krw-backend","server.js")
src=open(FILE,encoding="utf-8").read()
if "patch 327" in src:
    sys.exit("patch 327 already applied - nothing to do")
A="const TOKEN_TTL_MS  = 12 * 60 * 60 * 1000;"
R="const TOKEN_TTL_MS  = 30 * 24 * 60 * 60 * 1000;   // patch 327: 30 days (was 12h) - stop mid-task logouts"
if src.count(A)!=1: sys.exit("ABORT: anchor found %d times, expected 1."%src.count(A))
src=src.replace(A,R,1)
shutil.copyfile(FILE, FILE+".pre-327.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 327 applied to",FILE)
