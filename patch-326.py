#!/usr/bin/env python3
"""
patch-326.py  --  Leadbloom routes to NLD first, LT-Intake catches the rest

Kyler (Oct 14): every Leadbloom lead goes to NLD; if the state is one NLD
doesn't cover, it goes to LT-Intake only. Replaces the Oct 8 LT+NLD 50/50.
NLD is tried first for its 11 states (falling to LT on a reject/cap), and
any non-NLD-state lead goes straight to LT. CH never sees a Leadbloom lead.

Updated in all three routing paths so they stay consistent:
  1. LB_ONLY_LADDER   - the live nyc-split ladder for KRW-LEADBLOOM-MVA
  2. janitor retry    - overnight recovery of stuck/cap-rejected leads
  3. resend-ladder    - manual /leads/:id/resend-ladder

Backup: server.js.pre-326.bak    Safe to re-run: refuses twice.
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
if "patch 326" in src:
    sys.exit("patch 326 already applied - nothing to do")

def apply(s, anchor, repl, what):
    n=s.count(anchor)
    if n!=1: sys.exit("ABORT (%s): anchor found %d times, expected 1. No change."%(what,n))
    return s.replace(anchor, repl, 1)

# 1) live ladder
A1=("""  const LB_ONLY_LADDER = [
    { name: 'LT-Intake', priority: 1, group: 'lb5050', cap: null, payout: 2500, enabled: !!process.env.LT_INTAKE_PASS, states: 'ALL' },
    { name: 'NLD CPA',   priority: 1, group: 'lb5050', cap: 10,   payout: 2000, enabled: !NLD_PAUSED,                 states: NLD_ONLY_STATES },   // patch 318
  ];""")
R1=("""  const LB_ONLY_LADDER = [
    { name: 'NLD CPA',   priority: 1, group: 'nld',     cap: 10,   payout: 2000, enabled: !NLD_PAUSED,                 states: NLD_ONLY_STATES },   // patch 326: NLD first for its states
    { name: 'LT-Intake', priority: 2, group: 'lt-rest', cap: null, payout: 2500, enabled: !!process.env.LT_INTAKE_PASS, states: 'ALL' },            // patch 326: LT catches non-NLD states + NLD rejects
  ];""")
src=apply(src, A1, R1, "LB_ONLY_LADDER")

# 2) janitor
A2=("""        const wantNld = !NLD_PAUSED && JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX;   // patch 318
        const wantLt  = !!process.env.LT_INTAKE_PASS;
        if (wantNld && wantLt) { if (nldToday <= ltToday) rungs.push('NLD CPA', 'LT-Intake'); else rungs.push('LT-Intake', 'NLD CPA'); }
        else if (wantNld) rungs.push('NLD CPA');
        else if (wantLt)  rungs.push('LT-Intake');""")
R2=("""        const wantNld = !NLD_PAUSED && JAN_NLD_STATES.includes(st) && nldToday < 10 && sentNld < JAN_NLD_MAX;   // patch 318
        if (wantNld) rungs.push('NLD CPA');                        // patch 326: NLD first for its states
        if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake');   // patch 326: LT catches the rest""")
src=apply(src, A2, R2, "janitor LB routing")

# 3) resend-ladder
A3=("""      const wantNld = !NLD_PAUSED && NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10;   // patch 318
      const wantLt  = !!process.env.LT_INTAKE_PASS;
      if (wantNld && wantLt) { if (nldToday <= ltToday) rungs.push('NLD CPA', 'LT-Intake'); else rungs.push('LT-Intake', 'NLD CPA'); }
      else if (wantNld) rungs.push('NLD CPA');
      else if (wantLt)  rungs.push('LT-Intake');""")
R3=("""      const wantNld = !NLD_PAUSED && NLD_ONLY_STATES_GLOBAL.includes(st) && nldToday < 10;   // patch 318
      if (wantNld) rungs.push('NLD CPA');                        // patch 326: NLD first for its states
      if (process.env.LT_INTAKE_PASS) rungs.push('LT-Intake');   // patch 326: LT catches the rest""")
src=apply(src, A3, R3, "resend-ladder LB routing")

shutil.copyfile(FILE, FILE+".pre-326.bak")
open(FILE,"w",encoding="utf-8").write(src)
print("patch 326 applied to",FILE)
