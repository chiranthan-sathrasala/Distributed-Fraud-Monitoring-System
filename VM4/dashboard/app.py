"""Fraud monitoring dashboard (Streamlit). Run with VM4/run_dashboard.sh.

Sections: filters (time range, account, rule) -> KPIs -> charts -> flagged accounts and alert
tables -> investigation view for one alert. Any loading problem shows a clear error message.
"""
import os
import sys
import time

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import data  # noqa: E402

ALL_RULES = ["R1", "R2", "R3", "R4"]
ALERT_RULES = ["R2", "R3", "R4"]

st.set_page_config(page_title="Fraud monitoring", layout="wide")
st.title("Real-time fraud monitoring")


@st.cache_data(ttl=10, show_spinner="Loading data ...")
def get_data(run):
    return data.load_run(run)


# ------------------------------------------------------------------ sidebar: run + refresh
st.sidebar.header("Data")
run = st.sidebar.text_input("Run name", value=os.environ.get("DASHBOARD_RUN", "run1"),
                            help="Tables used: transaction_decisions_<run> and account_risk_alerts_<run>")
auto = st.sidebar.checkbox("Auto-refresh every 15 s", value=False)
if st.sidebar.button("Refresh now"):
    st.cache_data.clear()

try:
    decisions, alerts = get_data(run.strip())
except data.DataLoadError as e:
    st.error(f"Could not load the data for run '{run}'.\n\n**Reason:** {e}\n\n"
             "Check that HDFS on VM3 is running, the run name is right, the stream has written data "
             "and the Iceberg jar path (ICEBERG_JAR) is correct.")
    st.stop()

if decisions.empty and alerts.empty:
    st.info(f"Run '{run}' has no data yet. Start the producer and the Spark job, then press Refresh.")
    st.stop()

st.caption(f"Run **{run}** | {len(decisions)} decisions, {len(alerts)} alerts loaded | "
           f"last refresh {time.strftime('%H:%M:%S')}")

# ------------------------------------------------------------------ sidebar: filters
st.sidebar.header("Filters")
ts_min = decisions["event_timestamp"].min()
ts_max = decisions["event_timestamp"].max()
if pd.notna(ts_min) and pd.notna(ts_max) and ts_min < ts_max:
    start, end = st.sidebar.slider("Time range (event time)", min_value=ts_min.to_pydatetime(),
                                   max_value=ts_max.to_pydatetime(),
                                   value=(ts_min.to_pydatetime(), ts_max.to_pydatetime()),
                                   format="MM-DD HH:mm")
    start, end = pd.Timestamp(start), pd.Timestamp(end)
else:
    start, end = ts_min, ts_max

accounts_all = sorted(set(decisions["source_account"]) | set(alerts["source_account"]))
sel_accounts = st.sidebar.multiselect("Account (empty = all)", accounts_all)
sel_rules = st.sidebar.multiselect("Rule", ALL_RULES, default=ALL_RULES)

dec = decisions.copy()
alr = alerts.copy()
if pd.notna(start):
    dec = dec[(dec["event_timestamp"] >= start) & (dec["event_timestamp"] <= end)]
    alr = alr[(alr["window_end"] >= start) & (alr["window_start"] <= end)]
if sel_accounts:
    dec = dec[dec["source_account"].isin(sel_accounts)]
    alr = alr[alr["source_account"].isin(sel_accounts)]
if set(sel_rules) != set(ALL_RULES):
    mask = pd.Series(False, index=dec.index)
    for r in sel_rules:
        mask |= dec[f"{r.lower()}_triggered"]
    dec = dec[mask]
    alr = alr[alr["rule_id"].isin(sel_rules)]

# ------------------------------------------------------------------ KPIs
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Transactions", f"{len(dec):,}")
k2.metric("Alerts", f"{len(alr):,}")
k3.metric("Flagged accounts", f"{alr['source_account'].nunique():,}")
k4.metric("BLOCK decisions", f"{int((dec['decision'] == 'BLOCK').sum()):,}")
k5.metric("REVIEW decisions", f"{int((dec['decision'] == 'REVIEW').sum()):,}")

# ------------------------------------------------------------------ charts
c1, c2 = st.columns(2)
with c1:
    st.subheader("Alerts by rule")
    by_rule = alr["rule_id"].value_counts().reindex(ALERT_RULES, fill_value=0)
    st.bar_chart(by_rule)
with c2:
    st.subheader("Transactions and alerts per minute")
    tx_min = dec.set_index("event_timestamp").resample("1min").size().rename("transactions") \
        if len(dec) else pd.Series(dtype=int, name="transactions")
    al_min = alr.set_index("window_end").resample("1min").size().rename("alerts") \
        if len(alr) else pd.Series(dtype=int, name="alerts")
    per_min = pd.concat([tx_min, al_min], axis=1).fillna(0).astype(int)
    if per_min.empty:
        st.info("Nothing to chart for the current filters.")
    else:
        st.line_chart(per_min)
    st.caption("Alerts are counted at the minute their window ends.")

# ------------------------------------------------------------------ tables
st.subheader("Flagged accounts")
if alr.empty:
    st.info("No alerts for the current filters.")
else:
    flagged = (alr.groupby("source_account")
               .agg(alerts=("alert_id", "count"),
                    rules=("rule_id", lambda s: ", ".join(sorted(set(s)))),
                    first_window=("window_start", "min"),
                    last_window=("window_end", "max"))
               .reset_index())
    blocks = dec[dec["decision"] == "BLOCK"].groupby("source_account").size().rename("block_decisions")
    flagged = flagged.merge(blocks, on="source_account", how="left").fillna({"block_decisions": 0})
    flagged["block_decisions"] = flagged["block_decisions"].astype(int)
    st.dataframe(flagged.sort_values("alerts", ascending=False).reset_index(drop=True))

st.subheader("Alert details")
alert_cols = ["alert_id", "rule_id", "source_account", "destination_account", "trigger_transaction_id",
              "window_start", "window_end", "observed_count", "observed_amount_inr", "explanation"]
if alr.empty:
    st.info("No alerts for the current filters.")
else:
    st.dataframe(alr.sort_values("window_end", ascending=False)[alert_cols].reset_index(drop=True))

# ------------------------------------------------------------------ investigation view
st.subheader("Investigate an alert")
if alr.empty:
    st.info("Select filters that contain at least one alert to investigate it.")
else:
    options = alr.sort_values("window_end", ascending=False)["alert_id"].tolist()
    chosen = st.selectbox("Alert", options)
    a = alerts[alerts["alert_id"] == chosen].iloc[0]
    st.info(a["explanation"])
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Rule", a["rule_id"])
    m2.metric("Account", a["source_account"])
    m3.metric("Observed count", int(a["observed_count"]))
    m4.metric("Observed amount (INR)", "-" if pd.isna(a["observed_amount_inr"]) else f"{a['observed_amount_inr']:,.2f}")
    st.write(f"Window: **{a['window_start']}** to **{a['window_end']}**"
             + (f" | Destination: **{a['destination_account']}**" if a["destination_account"] else ""))

    tx_cols = ["transaction_id", "event_timestamp", "source_account", "destination_account", "amount",
               "currency", "r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered", "decision"]
    st.markdown("**Transaction that triggered the alert**")
    trig = decisions[decisions["transaction_id"] == a["trigger_transaction_id"]]
    if trig.empty:
        st.warning(f"Transaction {a['trigger_transaction_id']} has no decision row yet.")
    else:
        st.dataframe(trig[tx_cols].reset_index(drop=True))
    st.markdown(f"**Supporting transactions ({len(a['supporting_transaction_ids'])})**")
    sup = decisions[decisions["transaction_id"].isin(a["supporting_transaction_ids"])]
    st.dataframe(sup.sort_values("event_timestamp")[tx_cols].reset_index(drop=True))
    st.markdown(f"**Last 20 transactions of account {a['source_account']}**")
    last20 = (decisions[decisions["source_account"] == a["source_account"]]
              .sort_values("event_timestamp", ascending=False).head(20))
    st.dataframe(last20[tx_cols].reset_index(drop=True))

if auto:
    time.sleep(15)
    st.rerun()
