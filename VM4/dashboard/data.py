"""Data access for the dashboard. The rest of the app never touches Spark or files directly.

DASHBOARD_SOURCE=iceberg (default)  read the two Iceberg tables of a run through local-mode PySpark
DASHBOARD_SOURCE=csv                read CSV files (for developing the UI without the cluster):
                                    DASHBOARD_CSV_DIR must contain a file with 'transaction_decisions' and one with
                                    'account_risk_alerts' in the name (the ground-truth files work as they are)

Both return pandas frames with the same columns/types:
  decisions: transaction_id, source_account, destination_account, event_timestamp (datetime), evaluated_at,
             amount (float), currency, r1..r4_triggered (bool), r5_risk_score (int), r5_block_triggered (bool),
             decision, related_alert_ids (list)
  alerts:    alert_id, rule_id, source_account, destination_account, trigger_transaction_id, finding_scope,
             window_start, window_end (datetime), observed_count (int), observed_amount_inr (float or NaN),
             count_threshold, amount_threshold_inr, supporting_transaction_ids (list), explanation, triggered_at
"""
import glob
import json
import os

import pandas as pd

SOURCE = os.environ.get("DASHBOARD_SOURCE", "iceberg")
CATALOG = os.environ.get("ICEBERG_CATALOG", "iceberg_catalog")
DB = os.environ.get("ICEBERG_DB", "fraud")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "hdfs://192.168.193.59:9000/warehouse")
ICEBERG_JAR = os.environ.get("ICEBERG_JAR", "/opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar")

_spark = None


class DataLoadError(Exception):
    """Raised with a human-readable message when data cannot be loaded."""


def _json_list(v):
    if isinstance(v, (list, tuple)):
        return list(v)
    v = "" if v is None else str(v).strip()
    return json.loads(v) if v not in ("", "[]") else []


def _typed(dec, alr):
    for c in ("event_timestamp", "evaluated_at"):
        dec[c] = pd.to_datetime(dec[c].replace("", None), errors="coerce")
    for c in ("window_start", "window_end", "triggered_at"):
        alr[c] = pd.to_datetime(alr[c].replace("", None), errors="coerce")
    dec["amount"] = pd.to_numeric(dec["amount"], errors="coerce")
    for c in ("r1_triggered", "r2_triggered", "r3_triggered", "r4_triggered", "r5_block_triggered"):
        dec[c] = dec[c].astype(str).str.lower().eq("true")
    dec["r5_risk_score"] = pd.to_numeric(dec["r5_risk_score"], errors="coerce").fillna(0).astype(int)
    dec["related_alert_ids"] = dec["related_alert_ids"].map(_json_list)
    for c in ("observed_count", "count_threshold"):
        alr[c] = pd.to_numeric(alr[c], errors="coerce").fillna(0).astype(int)
    for c in ("observed_amount_inr", "amount_threshold_inr"):
        alr[c] = pd.to_numeric(alr[c], errors="coerce")
    alr["supporting_transaction_ids"] = alr["supporting_transaction_ids"].map(_json_list)
    alr["destination_account"] = alr["destination_account"].fillna("")
    return dec, alr


def _get_spark():
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = (SparkSession.builder.master("local[2]").appName("fraud-dashboard")
                  .config("spark.sql.session.timeZone", "UTC")
                  .config("spark.ui.enabled", "false")
                  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                  .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
                  .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
                  .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
                  .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
                  .config("spark.jars", ICEBERG_JAR)
                  .getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def _read_iceberg(table):
    from pyspark.sql import functions as F
    spark = _get_spark()
    df = spark.table(table)
    cols = []
    for f in df.schema.fields:                       # text everywhere: no time-zone surprises
        c = F.col(f.name)
        t = f.dataType.simpleString()
        if t == "timestamp":
            c = F.date_format(c, "yyyy-MM-dd HH:mm:ss")
        elif t.startswith("array"):
            c = F.to_json(c)
        else:
            c = c.cast("string")
        cols.append(F.coalesce(c, F.lit("")).alias(f.name))
    return df.select(*cols).toPandas()


def _find_csv(folder, name):
    """First CSV in the folder whose file name contains `name`."""
    hits = sorted(glob.glob(os.path.join(folder, f"*{name}*.csv")))
    if not hits:
        raise DataLoadError(f"no file with '{name}' in its name found in {folder}")
    return hits[0]


def load_run(run):
    """Return (decisions, alerts) for a run. Raises DataLoadError with a clear message."""
    try:
        if SOURCE == "csv":
            folder = os.environ.get("DASHBOARD_CSV_DIR", "")
            if not folder or not os.path.isdir(folder):
                raise DataLoadError("DASHBOARD_CSV_DIR is not set or is not a folder")
            dec = pd.read_csv(_find_csv(folder, "transaction_decisions"), dtype=str, keep_default_na=False)
            alr = pd.read_csv(_find_csv(folder, "account_risk_alerts"), dtype=str, keep_default_na=False)
        else:
            if not os.path.exists(ICEBERG_JAR):
                raise DataLoadError(f"Iceberg jar not found at {ICEBERG_JAR} - set ICEBERG_JAR "
                                    "(see VM4/README.md)")
            dec = _read_iceberg(f"{CATALOG}.{DB}.transaction_decisions_{run}")
            alr = _read_iceberg(f"{CATALOG}.{DB}.account_risk_alerts_{run}")
        return _typed(dec, alr)
    except DataLoadError:
        raise
    except Exception as e:                           # table missing, HDFS down, jar missing, ...
        first = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
        raise DataLoadError(f"{first[:300]}") from e
