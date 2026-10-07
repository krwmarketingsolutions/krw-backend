#!/usr/bin/env python3
"""
patch-295.py  --  Make the dashboard's login token work on EVERY endpoint

THE BUG (mine, from patch 288)
  Patch 288 taught requireKey() to accept the dashboard's Bearer token, but
  32 other endpoints read req.headers['x-api-key'] directly and were never
  updated - orAuth(), which guards the whole Outreach board, among them. With
  a perfectly valid token those endpoints returned 401.

  Alone that was invisible: a panel just stayed empty. Then patch 294 added
  "any 401 ends the session", and the two combined into a sign-in loop:

      sign in -> page loads -> Outreach calls /outreach/contacts
      -> orAuth rejects the token -> 401 -> session dropped
      -> "Your session is no longer valid. Sign in again."

  So signing in looked broken while the password was right all along.

THE FIX (one middleware, not 32 edits)
  Before any route runs: if a request carries no x-api-key but does carry a
  valid admin Bearer token, fill in the real API key for that request. Every
  existing check then passes untouched.

  Security is unchanged. A valid admin token already proves admin; this stops
  the server forgetting that halfway down the file. It will not:
    - overwrite an x-api-key the caller actually sent
    - act on an invalid, expired or tampered token (readToken verifies the
      HMAC and the expiry before this runs)
    - act on the scoped portal key, which stays locked out of admin routes

  It also removes the old leaked key that orAuth still carried as a fallback.
"""
import os, re, shutil, sys

TARGET = os.path.join(os.getcwd(), "server.js")
if not os.path.exists(TARGET):
    sys.exit("server.js not found - run this from ~/krw/krw-backend")

src = open(TARGET, encoding="utf-8").read()
if "patch 295" in src:
    sys.exit("patch 295 already applied - nothing to do")

m = re.search(r"^//[^\n]*end patch 288 auth[^\n]*$", src, re.M)
if not m:
    sys.exit("patch 288 auth block not found - run patch 288 first")
ANCHOR = m.group(0)

BRIDGE = r'''
// --- patch 295: one place where a login token becomes the API key ------------
// 32 endpoints in this file check req.headers['x-api-key'] by hand. Rather
// than edit all of them, a verified admin token is translated into the key
// once, before any route sees the request. readToken() has already checked
// the HMAC and the expiry, so this grants nothing a caller did not prove.
app.use((req, res, next) => {
  try {
    const sent = (req.headers['x-api-key'] || '').trim();
    if (!sent && API_KEY) {
      const t = readToken(_bearer(req));
      if (t && t.role === 'admin') req.headers['x-api-key'] = API_KEY;
    }
  } catch (e) { /* never block a request over this */ }
  next();
});
console.log('[Auth] patch 295 - admin login token now works on every endpoint');
// --- end patch 295 -----------------------------------------------------------
'''

src = src.replace(ANCHOR, ANCHOR + "\n" + BRIDGE, 1)

OLD = """function orAuth(req, res) {
  const key = req.headers['x-api-key'] || req.query.api_key || '';
  if (key !== (process.env.API_KEY || '64tgzb5ostadx1azjio9crdlduw4vf29')) { res.status(401).json({ ok: false, error: 'Invalid API key' }); return false; }
  return true;
}"""
NEW = """function orAuth(req, res) {
  // patch 295: same rule as every other admin route - key or login token.
  // The old hardcoded fallback key is gone; it was rotated and is public.
  if (callerRole(req) === 'admin') return true;
  res.status(401).json({ ok: false, error: 'Unauthorized' });
  return false;
}"""
if src.count(OLD) == 1:
    src = src.replace(OLD, NEW, 1)
    oa = "orAuth rewritten to accept the login token (old leaked fallback key removed)"
else:
    oa = "orAuth already updated or shaped differently - left alone"

shutil.copy2(TARGET, TARGET + ".pre-295.bak")
open(TARGET, "w", encoding="utf-8").write(src)
print("patch 295 applied. backup: server.js.pre-295.bak")
print("  " + oa)
print("  a verified admin token now satisfies all 32 hand-rolled key checks")
