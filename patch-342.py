#!/usr/bin/env python3
"""
patch-342.py  --  read publisher payout from the table that actually holds it

THE BUG
/funding-summary resolved publisher cost from publisher_campaigns.payout_rate.
The rates Kyler actually maintains live on publishers.payout_rate, which is
what patch 329's POST /publishers/:pub_id/payout-rate writes and what the MVA
portal pays from. publisher_campaigns is largely unset, so every lookup
returned 0.

The effect was not a missing number, it was a wrong one: cost came back $0 for
every publisher, so the P&L reported 100% margin and counted all revenue as
profit. On the live data that was $22,150 of "profit" against a true figure
nearer $4,250. Margin per lane was overstated the same way.

THE FIX
Rate resolution order, first match wins:
  1. publishers.payout_rate          - the flat per-publisher rate he maintains
  2. publisher_campaigns.payout_rate - campaign-specific, exact campaign match
  3. publisher_campaigns.payout_rate - any active row for that publisher

The flat rate is preferred because it is the one kept current and the one the
publisher portal pays from, so the P&L now agrees with what publishers are
actually owed.

A publisher with no rate anywhere still resolves to 0, and those rows stay out
of P&L and margin via the existing fsCostKnown guard rather than being counted
as free revenue.

Backup: server.js.pre-342.bak    Safe to re-run: refuses twice.
"""
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    for c in (HERE, os.path.dirname(HERE), os.getcwd(), os.path.dirname(os.getcwd())):
        if os.path.isdir(os.path.join(c, "krw-backend")):
            return c
    return None


ROOT = find_root() or sys.exit("run from ~/krw")
FILE = os.path.join(ROOT, "krw-backend", "server.js")
src = open(FILE, encoding="utf-8").read()

if "patch 342" in src:
    sys.exit("patch 342 already applied - nothing to do")
if "patch 341" not in src:
    sys.exit("patch 341 is not applied - run patch-341.py first")

OLD = """    const rateRows = await pool.query(
      `SELECT pub_id, campaign, COALESCE(payout_rate,0)::float AS payout_rate
         FROM publisher_campaigns WHERE active = true`);
    const rateFor = (pub, campaign) => {
      if (!pub) return 0;
      const mine = rateRows.rows.filter(r => r.pub_id === pub);
      if (!mine.length) return 0;
      const exact = mine.find(r => r.campaign === campaign);
      return fsNum((exact || mine[0]).payout_rate);
    };"""

NEW = """    /* patch 342: the rates Kyler maintains live on publishers.payout_rate (what
       POST /publishers/:pub_id/payout-rate writes and what the portal pays
       from). publisher_campaigns is largely unset, so reading only that table
       returned 0 for everyone and the P&L reported 100% margin. */
    const rateRows = await pool.query(
      `SELECT pub_id, campaign, COALESCE(payout_rate,0)::float AS payout_rate
         FROM publisher_campaigns WHERE active = true`);
    const flatRows = await pool.query(
      `SELECT pub_id, COALESCE(payout_rate,0)::float AS payout_rate FROM publishers`);
    const flatRate = {};
    flatRows.rows.forEach(r => { flatRate[r.pub_id] = fsNum(r.payout_rate); });

    const rateFor = (pub, campaign) => {
      if (!pub) return 0;
      if (flatRate[pub] > 0) return flatRate[pub];              // flat per-publisher rate
      const mine = rateRows.rows.filter(r => r.pub_id === pub);
      if (!mine.length) return 0;
      const exact = mine.find(r => r.campaign === campaign);    // campaign-specific
      if (exact && fsNum(exact.payout_rate) > 0) return fsNum(exact.payout_rate);
      const any = mine.find(r => fsNum(r.payout_rate) > 0);     // any active row
      return any ? fsNum(any.payout_rate) : 0;
    };"""

if src.count(OLD) != 1:
    sys.exit("rate lookup block found %d times, expected 1 - aborting, nothing changed" % src.count(OLD))

src = src.replace(OLD, NEW, 1)

shutil.copyfile(FILE, FILE + ".pre-342.bak")
open(FILE, "w", encoding="utf-8").write(src)
print("patch 342 applied - publisher payout now read from publishers.payout_rate")
print("backup at server.js.pre-342.bak")
