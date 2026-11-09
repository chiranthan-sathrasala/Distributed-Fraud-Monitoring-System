import argparse, re
from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--run", default="run1", help="run name, e.g. run1")
run = p.parse_args().run
if not re.fullmatch(r"run[0-9a-z]+", run):
    raise SystemExit("Run name must look like run1, run2, run1b ...")

spark = SparkSession.builder.appName(f"create_tables_{run}").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
cat = "iceberg_catalog.fraud"

steps = [
    ("namespace", f"CREATE NAMESPACE IF NOT EXISTS {cat}"),
    ("transaction_decisions", f"""
        CREATE TABLE IF NOT EXISTS {cat}.transaction_decisions_{run} (
          transaction_id STRING, source_account STRING, destination_account STRING,
          event_timestamp TIMESTAMP, evaluated_at TIMESTAMP,
          amount DECIMAL(18,2), currency STRING,
          r1_triggered BOOLEAN, r2_triggered BOOLEAN, r3_triggered BOOLEAN, r4_triggered BOOLEAN,
          r5_risk_score INT, r5_block_triggered BOOLEAN, decision STRING,
          related_alert_ids ARRAY<STRING>
        ) USING iceberg"""),
    ("account_risk_alerts", f"""
        CREATE TABLE IF NOT EXISTS {cat}.account_risk_alerts_{run} (
          alert_id STRING, rule_id STRING, source_account STRING, destination_account STRING,
          trigger_transaction_id STRING, finding_scope STRING,
          window_start TIMESTAMP, window_end TIMESTAMP,
          observed_count INT, observed_amount_inr DECIMAL(18,2),
          count_threshold INT, amount_threshold_inr DECIMAL(18,2),
          supporting_transaction_ids ARRAY<STRING>, explanation STRING,
          triggered_at TIMESTAMP
        ) USING iceberg"""),
]

for i, (name, sql) in enumerate(steps, 1):
    print(f"[{i}/{len(steps)}] creating {name} ...", flush=True)
    spark.sql(sql)

for t in (f"transaction_decisions_{run}", f"account_risk_alerts_{run}"):
    n = spark.sql(f"SELECT count(*) FROM {cat}.{t}").first()[0]
    print(f"OK  {cat}.{t}  rows={n}")
spark.stop()
