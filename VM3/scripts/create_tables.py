#!/usr/bin/env python3
"""Create the two Iceberg tables for a run (schemas come from common/schemas.py).

usage (on VM3, see VM3/scripts/create_run.sh):  create_tables.py --run run1
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from pyspark.sql import SparkSession  # noqa: E402

from common import schemas  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--run", default="run1", help="run name, e.g. run1")
p.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", "iceberg_catalog"))
p.add_argument("--db", default=os.environ.get("ICEBERG_DB", "fraud"))
args = p.parse_args()
run = args.run
if not re.fullmatch(r"run[0-9a-z]+", run):
    raise SystemExit("Run name must look like run1, run2, run1b ...")

spark = SparkSession.builder.appName(f"create_tables_{run}").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
decisions, alerts = schemas.table_names(run, args.catalog, args.db)

steps = [
    ("namespace", f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.db}"),
    ("transaction_decisions", f"CREATE TABLE IF NOT EXISTS {decisions} ({schemas.DECISIONS_COLUMNS}) USING iceberg"),
    ("account_risk_alerts", f"CREATE TABLE IF NOT EXISTS {alerts} ({schemas.ALERTS_COLUMNS}) USING iceberg"),
]
for i, (name, sql) in enumerate(steps, 1):
    print(f"[{i}/{len(steps)}] creating {name} ...", flush=True)
    spark.sql(sql)

for t in (decisions, alerts):
    n = spark.sql(f"SELECT count(*) FROM {t}").first()[0]
    print(f"OK  {t}  rows={n}")
spark.stop()
