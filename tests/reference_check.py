#!/usr/bin/env python3
"""Pandas re-implementation of the rules, checked against the ground truth (no Spark needed).

Shows that the rules as implemented in VM2/fraud_stream.py reproduce all 129 alerts and 1990 decisions.
usage: python3 tests/reference_check.py
"""
import json
import os
from decimal import Decimal

import pandas as pd

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
t = pd.read_csv(os.path.join(ROOT, "dataset/input/transactions.csv"), dtype=str).drop_duplicates("transaction_id")
t["ts"] = pd.to_datetime(t.event_timestamp.str.replace("Z", "", regex=False))
t["amt"] = t.amount.map(Decimal)
t["inr"] = t.currency.eq("INR")

rows = []
for r in t.itertuples():
    fl = r.ts.floor("30s")
    for back in (90, 60, 30, 0):
        rows.append((r.source_account, fl - pd.Timedelta(seconds=back), r.transaction_id))
m = pd.DataFrame(rows, columns=["src", "ws", "tid"]).merge(
    t[["transaction_id", "destination_account", "ts", "amt", "inr"]], left_on="tid", right_on="transaction_id")

alerts = []
for (src, ws), g in m.groupby(["src", "ws"]):
    g = g.sort_values(["ts", "tid"])
    if len(g) >= 5:
        alerts.append(("R2", src, "", ws, len(g), None, list(g.tid), g.tid.iloc[-1]))
    gi = g[g.inr]
    if len(gi) >= 2 and sum(gi.amt) > 600000:
        alerts.append(("R3", src, "", ws, len(gi), sum(gi.amt), list(gi.tid), gi.tid.iloc[-1]))
r4 = []
for (src, dst, ws), g in m.groupby(["src", "destination_account", "ws"]):
    g = g.sort_values(["ts", "tid"])
    if len(g) >= 3:
        r4.append(("R4", src, dst, ws, len(g), None, list(g.tid), g.tid.iloc[-1], g.ts.iloc[-1]))
r4 = pd.DataFrame(r4, columns=["rule_id", "src", "dst", "ws", "cnt", "amt", "ids", "trig", "trig_ts"])
r4 = r4.sort_values(["src", "ws", "cnt", "trig_ts", "dst"], ascending=[True, True, False, True, True]).drop_duplicates(["src", "ws"])
for r in r4.itertuples():
    alerts.append(("R4", r.src, r.dst, r.ws, r.cnt, None, r.ids, r.trig))
A = pd.DataFrame(alerts, columns=["rule_id", "src", "dst", "ws", "cnt", "amt", "ids", "trig"])
A["alert_id"] = A.rule_id + "-" + A.src + "-" + A.ws.dt.strftime("%Y%m%d%H%M%S")

gt = pd.read_csv(os.path.join(ROOT, "dataset/ground_truth/ground_truth_account_risk_alerts.csv"), dtype=str, keep_default_na=False)
gt["ws"] = pd.to_datetime(gt.window_start)
mine = {(a.rule_id, a.src, a.dst, a.ws, a.cnt, a.trig, tuple(a.ids), a.alert_id) for a in A.itertuples()}
theirs = {(g.rule_id, g.source_account, g.destination_account, g.ws, int(g.observed_count), g.trigger_transaction_id,
           tuple(json.loads(g.supporting_transaction_ids)), g.alert_id) for g in gt.itertuples()}
print(f"alerts: mine {len(mine)}, ground truth {len(theirs)}, differences {len(mine ^ theirs)}")

# decisions: account-level linking (alert applies to every payment of the account inside its window)
gd = pd.read_csv(os.path.join(ROOT, "dataset/ground_truth/ground_truth_transaction_decisions.csv"), dtype=str, keep_default_na=False)
bad = 0
by_acc = {k: v for k, v in A.groupby("src")}
for r in gd.itertuples():
    ts = pd.Timestamp(r.event_timestamp)
    a = by_acc.get(r.source_account)
    hit = a[(a.ws <= ts) & (ts < a.ws + pd.Timedelta(seconds=120))] if a is not None else A.iloc[0:0]
    rules = set(hit.rule_id)
    tr = t[t.transaction_id == r.transaction_id].iloc[0]
    r1 = bool(tr.inr and tr.amt >= 50000)
    score = int(r1) + sum(x in rules for x in ("R2", "R3", "R4"))
    dec = "BLOCK" if score >= 2 else "REVIEW" if score == 1 else "APPROVE"
    exp = (str(r1).lower(), str("R2" in rules).lower(), str("R3" in rules).lower(), str("R4" in rules).lower(),
           str(score), dec, tuple(sorted(hit.alert_id)))
    got = (r.r1_triggered, r.r2_triggered, r.r3_triggered, r.r4_triggered, r.r5_risk_score, r.decision,
           tuple(sorted(json.loads(r.related_alert_ids))))
    bad += exp != got
print(f"decisions: {len(gd)} checked, mismatches {bad}")
print("RESULT:", "MATCH - rules reproduce the ground truth" if not (mine ^ theirs) and bad == 0 else "DIFFERENCE")
