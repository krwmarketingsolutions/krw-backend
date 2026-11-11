#!/usr/bin/env python3
r"""
patch-385.py  --  /leads/feed returns when and how each disposition was set

For Kyler's personal dispositions portal (portal repo: Kyler0707/). The feed
already returns disp_status / disp_note. Add three read-only fields from the
same raw.buyer_disposition object so the portal can show when a buyer last
changed a lead and tell a real buyer rejection from an automatic age-out:

  disp_changed_at  - buyer_disposition.synced_at   (when the status last changed)
  disp_checked_at  - buyer_disposition.confirmed_at (when it was last confirmed)
  disp_source      - buyer_disposition.source       (buyer_sheet / aged_out / ...)

Additive only: no existing field changes, no routing or billing change.

Backup: server.js.pre-385.bak    Safe to re-run: refuses twice.
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
if "patch 385" in src:
    sys.exit("patch 385 already applied - nothing to do")

edits = [
("""              raw->'buyer_disposition'->>'status' as disp_status,
              raw->'buyer_disposition'->>'note'   as disp_note
       FROM leads ${wc} ORDER BY received_at DESC LIMIT $${i}`, params);""",
"""              raw->'buyer_disposition'->>'status' as disp_status,
              raw->'buyer_disposition'->>'note'   as disp_note,
              raw->'buyer_disposition'->>'synced_at'    as disp_changed_at,   -- patch 385
              raw->'buyer_disposition'->>'confirmed_at' as disp_checked_at,
              raw->'buyer_disposition'->>'source'       as disp_source
       FROM leads ${wc} ORDER BY received_at DESC LIMIT $${i}`, params);"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-385.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 385 applied")
