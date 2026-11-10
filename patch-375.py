#!/usr/bin/env python3
r"""
patch-375.py  --  trustedform-lookup also returns email + ip (read-only)

Extend /leads/trustedform-lookup to return each lead's email and IP address
alongside the TrustedForm cert, for enriching aged-lead exports.

  krw-backend/server.js (two edits, inside the patch-374 endpoint).

Backup: server.js.pre-375.bak    Safe to re-run: refuses twice.
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
if "patch 375" in src:
    sys.exit("patch 375 already applied - nothing to do")

edits = [
("              id, first_name, last_name, status, received_at,\n"
 "              COALESCE(NULLIF(raw->>'trustedform_cert_url', ''), NULLIF(raw->>'trusted_form_cert_url', '')) AS tf",
 "              id, first_name, last_name, status, received_at, email,\n"
 "              COALESCE(NULLIF(ip_address, ''), NULLIF(raw->>'ip_address', '')) AS ip,   -- patch 375\n"
 "              COALESCE(NULLIF(raw->>'trustedform_cert_url', ''), NULLIF(raw->>'trusted_form_cert_url', '')) AS tf"),
("      phone: p,\n"
 "      trustedform: (map[p] && map[p].tf) || '',\n"
 "      krw_id: (map[p] && map[p].id) || null,",
 "      phone: p,\n"
 "      trustedform: (map[p] && map[p].tf) || '',\n"
 "      email: (map[p] && map[p].email) || '',\n"
 "      ip: (map[p] && map[p].ip) || '',\n"
 "      krw_id: (map[p] && map[p].id) || null,"),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-375.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 375 applied")
