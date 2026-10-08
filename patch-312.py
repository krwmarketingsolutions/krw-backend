#!/usr/bin/env python3
"""
patch-312.py  --  Board tracker follows the new draft email subject

Kyler changed the draft agent's email subject from
    LinkedIn draft: <name> | <firm>
to
    NEW LINKDN CONNECTION - SEND THIS MESSAGE | <name> | <firm>
The patch 310 Sent Mail scanner matched only the old prefix, so new drafts
would stop landing on the Outreach board. It now accepts both subject
shapes: the name is the first pipe-separated part after the prefix, the
firm the second.

Usage (from ~/krw/krw-backend):
    python3 patch-312.py
    node --check server.js
    git add -A && git commit -m "patch 312: tracker follows new subject" && git push

Backup: server.js.pre-312.bak. Safe to re-run.
"""
import os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "server.js")
if not os.path.exists(TARGET):
    TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 312" in src:
    sys.exit("patch 312 already applied - nothing to do")

OLD = """        const subj = String((msg.envelope || {}).subject || '');
        if (subj.indexOf('LinkedIn draft:') !== 0) continue;
        const rest = subj.slice('LinkedIn draft:'.length).trim();
        const parts = rest.split('|').map(s => s.trim()).filter(Boolean);
        const name = parts[0] || '';
        const company = parts[1] || '';"""

if src.count(OLD) != 1:
    sys.exit("the subject parser does not look the way patch 312 expects - aborting, server.js untouched")

NEW = """        const subj = String((msg.envelope || {}).subject || '');
        // patch 312: two subject shapes are in the wild -
        //   "LinkedIn draft: <name> | <firm>"                          (old)
        //   "NEW LINKDN CONNECTION - SEND THIS MESSAGE | <name> | <firm>" (current)
        let rest = null;
        if (subj.indexOf('LinkedIn draft:') === 0) {
          rest = subj.slice('LinkedIn draft:'.length).trim();
        } else if (subj.toUpperCase().indexOf('NEW LINKDN CONNECTION') === 0 || subj.toUpperCase().indexOf('NEW LINKEDIN CONNECTION') === 0) {
          const firstPipe = subj.indexOf('|');
          if (firstPipe < 0) continue;
          rest = subj.slice(firstPipe + 1).trim();
        } else {
          continue;
        }
        const parts = rest.split('|').map(s => s.trim()).filter(Boolean);
        const name = parts[0] || '';
        const company = parts[1] || '';"""

src = src.replace(OLD, NEW, 1)
shutil.copy2(TARGET, os.path.join(os.path.dirname(TARGET), "server.js.pre-312.bak"))
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 312 applied. backup: server.js.pre-312.bak")
print("  board tracker now matches both draft subject formats")
