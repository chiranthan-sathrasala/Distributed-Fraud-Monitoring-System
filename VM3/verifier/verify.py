#!/usr/bin/env python3
"""Verifier: compares a run's Iceberg output with the ground truth and prints PASS / FAIL.

Run it on VM3 after every run (see VM3/verifier/run_verify.sh).

What it checks
  1. both tables exist and are not empty
  2. no duplicate transaction_id in decisions, no duplicate alert (alert_id or alert key)
  3. number of decisions / alerts equals the ground truth
  4. every decision matches: rule flags, risk score, block flag, decision, related alert ids
  5. every alert matches: key (rule, account, destination, window), counts, amounts, trigger
     transaction, supporting transactions
Fields that are expected to differ (evaluated_at, triggered_at, explanation text) are ignored.

Outputs: console report, WhatsApp-style plain-text summary, full report file and a CSV of all
differences in VM3/reports/.  Exit code: 0 = PASS, 1 = FAIL, 2 = could not run (e.g. table missing).
"""
import argparse
import json
import os
import sys
import time
from decimal import Decimal

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)

TRUTH_DIR = os.path.join(ROOT, "dataset", "ground_truth")
DEC_COLS = ["transaction_id", "source_account", "destination_account", "event_timestamp", "evaluated_at",
            "amount", "currency", "r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered",
            "r5_risk_score", "r5_block_triggered", "decision", "related_alert_ids"]
ALERT_COLS = ["alert_id", "rule_id", "source_account", "destination_account", "trigger_transaction_id",
              "finding_scope", "window_start", "window_end", "observed_count", "observed_amount_inr",
              "count_threshold", "amount_threshold_inr", "supporting_transaction_ids", "explanation",
              "triggered_at"]
TOTAL_STEPS = 7


def step(n, text):
    print(f"[{n}/{TOTAL_STEPS}] {text} ...", flush=True)


# ------------------------------------------------------------------ loading
def load_csv(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def load_iceberg(run, catalog, db):
    """Read both tables through Spark and hand back plain-string pandas frames.

    Timestamps are formatted to text inside Spark (session time zone = UTC) so no time-zone
    shift can sneak in when the rows are converted to pandas.
    """
    from pyspark.sql import SparkSession, functions as F
    from common import schemas

    spark = (SparkSession.builder.appName(f"verify-{run}")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    out = []
    for table in schemas.table_names(run, catalog, db):
        try:
            df = spark.table(table)
        except Exception as e:                                   # table missing
            raise RuntimeError(f"cannot read table {table}: {str(e).splitlines()[0]}")
        cols = []
        for f in df.schema.fields:
            c = F.col(f.name)
            t = f.dataType.simpleString()
            if t == "timestamp":
                c = F.date_format(c, "yyyy-MM-dd HH:mm:ss")
            elif t.startswith("array"):
                c = F.to_json(c)
            else:
                c = c.cast("string")
            cols.append(F.coalesce(c, F.lit("")).alias(f.name))
        out.append(df.select(*cols).toPandas())
    spark.stop()
    return out[0], out[1]


def load_parquet(dir_, run):
    dec = pd.read_parquet(os.path.join(dir_, f"transaction_decisions_{run}"))
    alr = pd.read_parquet(os.path.join(dir_, f"account_risk_alerts_{run}"))

    def fix(df):
        for c in df.columns:
            if df[c].dtype == object and len(df) and hasattr(df[c].iloc[0], "tolist"):
                df[c] = df[c].map(lambda v: json.dumps(list(v)))
            elif str(df[c].dtype).startswith("datetime"):
                df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
            else:
                df[c] = df[c].map(lambda v: "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v))
        return df
    return fix(dec), fix(alr)


# ------------------------------------------------------------------ normalising
def num2(v):
    v = "" if v is None else str(v).strip()
    return "" if v == "" else format(Decimal(v), ".2f")


def as_list(v):
    v = "" if v is None else str(v).strip()
    return tuple(sorted(json.loads(v))) if v not in ("", "[]") else tuple()


def boolean(v):
    return str(v).strip().lower()


def ts(v):
    v = "" if v is None else str(v).strip().replace("T", " ").replace("Z", "")
    return v[:19]


def norm_decisions(df):
    d = pd.DataFrame({
        "transaction_id": df["transaction_id"].str.strip(),
        "source_account": df["source_account"].str.strip(),
        "destination_account": df["destination_account"].str.strip(),
        "event_timestamp": df["event_timestamp"].map(ts),
        "amount": df["amount"].map(num2),
        "currency": df["currency"].str.strip(),
        "r1_triggered": df["r1_triggered"].map(boolean), "r2_triggered": df["r2_triggered"].map(boolean),
        "r3_triggered": df["r3_triggered"].map(boolean), "r4_triggered": df["r4_triggered"].map(boolean),
        "r5_risk_score": df["r5_risk_score"].astype(str).str.strip(),
        "r5_block_triggered": df["r5_block_triggered"].map(boolean),
        "decision": df["decision"].str.strip(),
        "related_alert_ids": df["related_alert_ids"].map(as_list),
    })
    return d


def norm_alerts(df):
    a = pd.DataFrame({
        "alert_id": df["alert_id"].str.strip(),
        "rule_id": df["rule_id"].str.strip(),
        "source_account": df["source_account"].str.strip(),
        "destination_account": df["destination_account"].str.strip(),
        "trigger_transaction_id": df["trigger_transaction_id"].str.strip(),
        "finding_scope": df["finding_scope"].str.strip(),
        "window_start": df["window_start"].map(ts),
        "window_end": df["window_end"].map(ts),
        "observed_count": df["observed_count"].astype(str).str.strip(),
        "observed_amount_inr": df["observed_amount_inr"].map(num2),
        "count_threshold": df["count_threshold"].astype(str).str.strip(),
        "amount_threshold_inr": df["amount_threshold_inr"].map(num2),
        "supporting_transaction_ids": df["supporting_transaction_ids"].map(as_list),
    })
    a["key"] = (a["rule_id"] + "|" + a["source_account"] + "|" + a["destination_account"] + "|" +
                a["window_start"] + "|" + a["window_end"])
    return a


# ------------------------------------------------------------------ comparing
def compare(truth, actual, key, fields, kind):
    """Return (missing, extra, mismatched) as lists of dicts."""
    t = truth.drop_duplicates(key).set_index(key)
    a = actual.drop_duplicates(key).set_index(key)
    missing = [k for k in t.index if k not in a.index]
    extra = [k for k in a.index if k not in t.index]
    both = [k for k in t.index if k in a.index]
    mism = []
    for k in both:
        bad = [f for f in fields if t.at[k, f] != a.at[k, f]]
        if bad:
            mism.append({"id": k, "fields": bad,
                         "expected": {f: t.at[k, f] for f in bad}, "actual": {f: a.at[k, f] for f in bad}})
    return missing, extra, mism


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", required=True, help="run name, e.g. run1")
    p.add_argument("--source", choices=["iceberg", "csv", "parquet"], default="iceberg")
    p.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", "iceberg_catalog"))
    p.add_argument("--db", default=os.environ.get("ICEBERG_DB", "fraud"))
    p.add_argument("--truth-dir", default=TRUTH_DIR)
    p.add_argument("--actual-decisions", help="CSV with decisions (source=csv)")
    p.add_argument("--actual-alerts", help="CSV with alerts (source=csv)")
    p.add_argument("--parquet-dir", help="folder with parquet output (source=parquet)")
    p.add_argument("--disorder-log", help="CSV from the producer (transaction_id,...,delay_seconds); "
                                          "used to explain differences in Experiment 3/4")
    p.add_argument("--report-dir", default=os.path.join(ROOT, "VM3", "reports"))
    p.add_argument("--label", default="", help="free text shown in the summary, e.g. 'Exp 1 executor kill'")
    a = p.parse_args()
    t_start = time.time()

    step(1, "loading ground truth")
    truth_dec = load_csv(os.path.join(a.truth_dir, "ground_truth_transaction_decisions.csv"))
    truth_alr = load_csv(os.path.join(a.truth_dir, "ground_truth_account_risk_alerts.csv"))

    step(2, f"loading output of run '{a.run}' from {a.source}")
    try:
        if a.source == "iceberg":
            act_dec, act_alr = load_iceberg(a.run, a.catalog, a.db)
        elif a.source == "parquet":
            act_dec, act_alr = load_parquet(a.parquet_dir, a.run)
        else:
            act_dec, act_alr = load_csv(a.actual_decisions), load_csv(a.actual_alerts)
    except Exception as e:
        print(f"\nVERDICT: ERROR - could not load the run output.\n  {e}")
        sys.exit(2)

    step(3, "normalising values")
    td, ad = norm_decisions(truth_dec), norm_decisions(act_dec)
    ta, aa = norm_alerts(truth_alr), norm_alerts(act_alr)

    checks = []                                     # (name, passed, detail)
    step(4, "basic health checks")
    checks.append(("decisions table not empty", len(ad) > 0, f"{len(ad)} rows"))
    checks.append(("alerts table not empty", len(aa) > 0, f"{len(aa)} rows"))
    dup_dec = int(ad["transaction_id"].duplicated().sum())
    dup_alr_id = int(aa["alert_id"].duplicated().sum())
    dup_alr_key = int(aa["key"].duplicated().sum())
    checks.append(("no duplicate transaction_id in decisions", dup_dec == 0, f"{dup_dec} duplicates"))
    checks.append(("no duplicate alerts", dup_alr_id == 0 and dup_alr_key == 0,
                   f"{dup_alr_id} duplicate alert_id, {dup_alr_key} duplicate alert keys"))

    step(5, "comparing decisions with ground truth")
    dec_fields = ["source_account", "destination_account", "event_timestamp", "amount", "currency",
                  "r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered", "r5_risk_score",
                  "r5_block_triggered", "decision", "related_alert_ids"]
    d_missing, d_extra, d_mism = compare(td, ad, "transaction_id", dec_fields, "decision")
    checks.append(("decision count equals ground truth", len(ad.drop_duplicates('transaction_id')) == len(td),
                   f"expected {len(td)}, got {len(ad.drop_duplicates('transaction_id'))}"))
    checks.append(("no missing decisions", not d_missing, f"{len(d_missing)} missing"))
    checks.append(("no extra decisions", not d_extra, f"{len(d_extra)} extra"))
    checks.append(("all decisions match", not d_mism, f"{len(d_mism)} mismatched"))

    step(6, "comparing alerts with ground truth")
    alr_fields = ["alert_id", "trigger_transaction_id", "finding_scope", "observed_count", "observed_amount_inr",
                  "count_threshold", "amount_threshold_inr", "supporting_transaction_ids"]
    a_missing, a_extra, a_mism = compare(ta, aa, "key", alr_fields, "alert")
    checks.append(("alert count equals ground truth", len(aa.drop_duplicates('key')) == len(ta),
                   f"expected {len(ta)}, got {len(aa.drop_duplicates('key'))}"))
    checks.append(("no missing alerts", not a_missing, f"{len(a_missing)} missing"))
    checks.append(("no extra alerts", not a_extra, f"{len(a_extra)} extra"))
    checks.append(("all alerts match", not a_mism, f"{len(a_mism)} mismatched"))

    step(7, "writing reports")
    passed = all(c[1] for c in checks)

    # --- optional: relate differences to delayed transactions
    delayed_note = []
    if a.disorder_log and os.path.exists(a.disorder_log):
        dl = pd.read_csv(a.disorder_log, dtype=str)
        delayed = set(dl["transaction_id"])
        dec_diff_ids = set(d_missing) | {m["id"] for m in d_mism}
        in_delayed = len(dec_diff_ids & delayed)
        delayed_note.append(f"delayed transactions in log: {len(delayed)}")
        delayed_note.append(f"differing decisions that are delayed transactions: {in_delayed} of {len(dec_diff_ids)}")
        sup = {}
        for df in (ta, aa):                       # union of expected and actual supporting transactions
            for k, v in zip(df["key"], df["supporting_transaction_ids"]):
                sup.setdefault(k, set()).update(v)
        alr_diff_keys = set(a_missing) | set(a_extra) | {m["id"] for m in a_mism}
        touching = sum(1 for k in alr_diff_keys if sup.get(k, set()) & delayed)
        delayed_note.append(f"differing alerts involving a delayed transaction: {touching} of {len(alr_diff_keys)}")

    mix = lambda df: {k: int(v) for k, v in df["decision"].value_counts().items()}
    lines = []
    lines.append(f"FRAUD VERIFIER REPORT - run {a.run} {('(' + a.label + ')') if a.label else ''}".strip())
    lines.append(f"generated {time.strftime('%Y-%m-%d %H:%M:%S')} | source: {a.source}")
    lines.append("")
    for name, ok, detail in checks:
        lines.append(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    lines.append("")
    lines.append(f"Decision mix expected {mix(td)} | actual {mix(ad)}")
    by_rule = lambda df: {k: int(v) for k, v in sorted(df["rule_id"].value_counts().items())}
    lines.append(f"Alerts by rule expected {by_rule(ta)} | actual {by_rule(aa)}")
    lines += delayed_note
    verdict = "PASS" if passed else "FAIL"
    lines.append("")
    lines.append(f"VERDICT: {verdict}")

    detail = ["", "---- details (first 10 of each) ----"]
    detail.append(f"missing decisions: {d_missing[:10]}")
    detail.append(f"extra decisions: {d_extra[:10]}")
    for m in d_mism[:10]:
        detail.append(f"mismatched decision {m['id']}: expected {m['expected']} actual {m['actual']}")
    detail.append(f"missing alerts: {a_missing[:10]}")
    detail.append(f"extra alerts: {a_extra[:10]}")
    for m in a_mism[:10]:
        detail.append(f"mismatched alert {m['id']}: expected {m['expected']} actual {m['actual']}")

    summary = [
        f"Fraud verifier - {a.run}" + (f" ({a.label})" if a.label else ""),
        f"VERDICT: {verdict}",
        f"Decisions: expected {len(td)}, got {len(ad)}, missing {len(d_missing)}, extra {len(d_extra)}, "
        f"mismatched {len(d_mism)}, duplicates {dup_dec}",
        f"Alerts: expected {len(ta)}, got {len(aa)}, missing {len(a_missing)}, extra {len(a_extra)}, "
        f"mismatched {len(a_mism)}, duplicates {dup_alr_id}",
        f"Decision mix: APPROVE {mix(ad).get('APPROVE', 0)}, REVIEW {mix(ad).get('REVIEW', 0)}, "
        f"BLOCK {mix(ad).get('BLOCK', 0)} (expected {mix(td).get('APPROVE', 0)}/{mix(td).get('REVIEW', 0)}/{mix(td).get('BLOCK', 0)})",
    ] + delayed_note

    os.makedirs(a.report_dir, exist_ok=True)
    report_path = os.path.join(a.report_dir, f"verify_{a.run}.txt")
    diffs_path = os.path.join(a.report_dir, f"verify_{a.run}_diffs.csv")
    with open(report_path, "w") as f:
        f.write("\n".join(lines + detail + ["", "---- WhatsApp summary ----"] + summary) + "\n")
    rows = ([("decision", "missing", k, "", "", "") for k in d_missing] +
            [("decision", "extra", k, "", "", "") for k in d_extra] +
            [("decision", "mismatch", m["id"], ",".join(m["fields"]), json.dumps(m["expected"]), json.dumps(m["actual"])) for m in d_mism] +
            [("alert", "missing", k, "", "", "") for k in a_missing] +
            [("alert", "extra", k, "", "", "") for k in a_extra] +
            [("alert", "mismatch", m["id"], ",".join(m["fields"]), json.dumps(m["expected"]), json.dumps(m["actual"])) for m in a_mism])
    pd.DataFrame(rows, columns=["kind", "type", "id", "fields", "expected", "actual"]).to_csv(diffs_path, index=False)

    print()
    print("\n".join(lines))
    if not passed:
        print("\n".join(detail[:6]))
    print()
    print("=" * 60)
    print("WhatsApp summary (copy and paste):")
    print("-" * 60)
    print("\n".join(summary))
    print("=" * 60)
    print(f"full report : {report_path}\ndifferences : {diffs_path}\nfinished in {time.time() - t_start:.1f}s")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
