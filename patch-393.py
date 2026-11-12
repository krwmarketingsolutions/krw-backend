#!/usr/bin/env python3
r"""
patch-393.py  --  admin can preview exactly what a partner login gets

Kyler, Nov 12: the funnel is not loading for Andrew and there was no way to
reproduce a partner request without his password.

An admin request may now add ?view_as=partner to any GET. The request then
runs through the SAME partner gate as a real partner login (read only,
allow-listed, MVA only, Oct 21 floor, filters) and returns what Andrew would
get. It only ever narrows what the admin sees; it is ignored for anyone who is
not already an admin.

Backup: server.js.pre-393.bak    Safe to re-run: refuses twice.
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
if "patch 393" in src:
    sys.exit("patch 393 already applied - nothing to do")

edits = [
("""  if (!t || t.role !== 'partner') return next();
  const deny = () => res.status(200).json({ ok: false, restricted: true, error: 'Not available for this login' });
  try {
    const u = (await dashUsersLoad())[t.uid];
    if (!u || !u.active || u.role !== 'partner' || Number(u.pass_ver) !== Number(t.pv)) {
      return res.status(401).json({ ok: false, error: 'This login is no longer active. Sign in again.' });
    }
    req.krwUser = { id: u.id, name: u.name, role: 'partner' };""",
 """  const isPartner = !!(t && t.role === 'partner');
  // patch 393: an admin may preview the partner view with ?view_as=partner (it only narrows what they get)
  const preview = !isPartner && req.method === 'GET' && req.query && req.query.view_as === 'partner' && callerRole(req) === 'admin';
  if (!isPartner && !preview) return next();
  const deny = () => res.status(200).json({ ok: false, restricted: true, error: 'Not available for this login' });
  try {
    if (preview) {
      req.krwUser = { id: 0, name: 'Preview', role: 'partner', preview: true };
    } else {
      const u = (await dashUsersLoad())[t.uid];
      if (!u || !u.active || u.role !== 'partner' || Number(u.pass_ver) !== Number(t.pv)) {
        return res.status(401).json({ ok: false, error: 'This login is no longer active. Sign in again.' });
      }
      req.krwUser = { id: u.id, name: u.name, role: 'partner' };
    }"""),
("""  if (!role && req.krwUser) return res.json({ ok: true, role: 'partner', name: req.krwUser.name,""",
 """  if (req.krwUser) return res.json({ ok: true, role: 'partner', preview: !!req.krwUser.preview, name: req.krwUser.name,"""),
]
for i,(a,r) in enumerate(edits,1):
    if src.count(a)!=1: sys.exit("ABORT: edit %d anchor found %d times, expected 1." % (i, src.count(a)))
for a,r in edits: src=src.replace(a,r,1)
shutil.copyfile(FILE, FILE+".pre-393.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 393 applied")
