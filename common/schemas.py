"""Single source of truth for the two Iceberg table schemas (assignment appendix)."""

DECISIONS_COLUMNS = """
  transaction_id STRING, source_account STRING, destination_account STRING,
  event_timestamp TIMESTAMP, evaluated_at TIMESTAMP,
  amount DECIMAL(18,2), currency STRING,
  r1_triggered BOOLEAN, r2_triggered BOOLEAN, r3_triggered BOOLEAN, r4_triggered BOOLEAN,
  r5_risk_score INT, r5_block_triggered BOOLEAN, decision STRING,
  related_alert_ids ARRAY<STRING>
"""

ALERTS_COLUMNS = """
  alert_id STRING, rule_id STRING, source_account STRING, destination_account STRING,
  trigger_transaction_id STRING, finding_scope STRING,
  window_start TIMESTAMP, window_end TIMESTAMP,
  observed_count INT, observed_amount_inr DECIMAL(18,2),
  count_threshold INT, amount_threshold_inr DECIMAL(18,2),
  supporting_transaction_ids ARRAY<STRING>, explanation STRING,
  triggered_at TIMESTAMP
"""


def table_names(run, catalog="iceberg_catalog", db="fraud"):
    """Return (decisions_table, alerts_table) for a run such as 'run1'."""
    return (f"{catalog}.{db}.transaction_decisions_{run}",
            f"{catalog}.{db}.account_risk_alerts_{run}")
