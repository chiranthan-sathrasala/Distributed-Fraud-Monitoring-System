#!/usr/bin/env python3
"""Real-time fraud monitoring job (PySpark Structured Streaming, written for Spark 3.3.x).

Flow:  Kafka -> parse -> watermark (1 min) -> 2-min/30-s sliding window per source account
       -> foreachBatch: build R2/R3/R4 alerts + one decision per transaction -> Iceberg.

Why foreachBatch with ONE aggregation (and not several chained stateful operators)?
Spark 3.3 cannot chain stateful operators (aggregation -> join) in append mode. So the only
stateful step is a single windowed aggregation that collects every transaction of an
(account, window). Everything else (R2/R3/R4, decisions) is ordinary batch logic on the windows
that just closed. See docs/DESIGN.md for the full reasoning.

Idempotent writes: both tables are written with MERGE ... WHEN NOT MATCHED THEN INSERT, keyed on
alert_id / transaction_id. If a micro-batch is replayed after a crash (Experiment 2) nothing is
duplicated.
"""
import argparse
import os
import sys
import time
from functools import reduce

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql import SparkSession, Window, functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

from common import schemas  # noqa: E402

FLUSH_ACCOUNT = "__FLUSH__"
WINDOW, SLIDE, WATERMARK = "2 minutes", "30 seconds", "1 minute"
R1_AMOUNT = 50000
R2_MIN_COUNT = 5
R3_MIN_COUNT, R3_MIN_TOTAL = 2, "600000.00"
R4_MIN_COUNT = 3

MSG_SCHEMA = StructType([StructField(n, StringType()) for n in
                         ("transaction_id", "source_account", "destination_account",
                          "event_timestamp", "amount", "currency")])


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", default="run1", help="run name: topic, checkpoint and tables are derived from it")
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default=None, help="default: transactions-<run>")
    p.add_argument("--starting-offsets", default="earliest")
    p.add_argument("--input-dir", default=None, help="folder of JSON-lines files (source=files)")
    p.add_argument("--max-files-per-trigger", type=int, default=1)
    p.add_argument("--checkpoint-base", default=os.environ.get("CHECKPOINT_BASE", "/tmp/checkpoints"),
                   help="MUST be an HDFS path on the real cluster")
    p.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", "iceberg_catalog"))
    p.add_argument("--db", default=os.environ.get("ICEBERG_DB", "fraud"))
    p.add_argument("--sink", choices=["iceberg", "parquet"], default="iceberg",
                   help="parquet is only for local tests without Iceberg")
    p.add_argument("--out-dir", default=None, help="folder for the parquet test sink")
    p.add_argument("--trigger", default="3 seconds", help="micro-batch trigger interval")
    p.add_argument("--shuffle-partitions", type=int, default=4)
    p.add_argument("--idle-exit", type=int, default=0,
                   help="stop automatically N seconds after the last input arrived (0 = run until Ctrl+C)")
    return p.parse_args()


# ----------------------------------------------------------------------------- sinks
# IMPORTANT: inside foreachBatch Spark hands us a DataFrame that belongs to a CLONED session.
# Temp views are per session, so every sink call receives that session (df.sparkSession) and uses it.
class IcebergSink:
    def __init__(self, catalog, db, run):
        self.decisions, self.alerts = schemas.table_names(run, catalog, db)

    def upsert(self, session, df, which, key):
        """INSERT rows whose key is not in the table yet (idempotent, safe to replay)."""
        table = self.decisions if which == "decisions" else self.alerts
        cols = [F.col(f.name).cast(f.dataType) for f in session.table(table).schema.fields]
        df.select(*cols).dropDuplicates([key]).createOrReplaceTempView("fraud_stage")
        session.sql(f"MERGE INTO {table} t USING fraud_stage s ON t.{key} = s.{key} "
                    f"WHEN NOT MATCHED THEN INSERT *")

    def read_alerts(self, session):
        session.sql(f"REFRESH TABLE {self.alerts}")
        return session.table(self.alerts)


class ParquetSink:
    """Test-only sink with the same behaviour (insert-if-absent) as the Iceberg MERGE."""

    def __init__(self, out_dir, run):
        self.out = out_dir
        self.cols = {"decisions": schemas.DECISIONS_COLUMNS, "alerts": schemas.ALERTS_COLUMNS}
        self.name = {"decisions": f"transaction_decisions_{run}", "alerts": f"account_risk_alerts_{run}"}

    def _path(self, which):
        return os.path.join(self.out, self.name[which])

    def _read(self, session, which):
        if os.path.exists(self._path(which)):
            return session.read.parquet(self._path(which))
        return session.createDataFrame([], self.cols[which])

    def upsert(self, session, df, which, key):
        existing = self._read(session, which)
        new = df.select(*[F.col(f.name).cast(f.dataType) for f in existing.schema.fields]).dropDuplicates([key])
        new = new.join(existing.select(key), key, "left_anti")
        new.write.mode("append").parquet(self._path(which))

    def read_alerts(self, session):
        return self._read(session, "alerts")


# ----------------------------------------------------------------------------- rules
def build_alerts(w, rule):
    """w = one row per (account, window, transaction). Returns alert rows for one rule."""
    keys = ["source_account", "ws", "we"]
    base = w
    if rule == "R3":                                    # R3 looks at INR payments only
        base = w.where(F.col("currency") == "INR")
    if rule == "R4":                                    # R4 is per (account, destination)
        keys = ["source_account", "destination_account", "ws", "we"]
    g = base.groupBy(*keys).agg(
        F.count(F.lit(1)).cast("int").alias("observed_count"),
        F.sum("amount").alias("total"),
        F.sort_array(F.collect_list(F.struct("event_ts", "transaction_id"))).alias("items"))

    if rule == "R2":
        g = g.where(F.col("observed_count") >= R2_MIN_COUNT)
    elif rule == "R3":
        g = g.where((F.col("observed_count") >= R3_MIN_COUNT) & (F.col("total") > F.lit(R3_MIN_TOTAL).cast("decimal(18,2)")))
    else:
        g = g.where(F.col("observed_count") >= R4_MIN_COUNT)
        # one R4 alert per (account, window): keep the destination with the most payments,
        # then the earliest trigger, then the lowest destination number (matches ground truth)
        last = F.element_at("items", -1)
        spec = Window.partitionBy("source_account", "ws").orderBy(
            F.col("observed_count").desc(), last.getField("event_ts").asc(), F.col("destination_account").asc())
        g = g.withColumn("_rn", F.row_number().over(spec)).where("_rn = 1").drop("_rn")

    trigger_id = F.element_at("items", -1).getField("transaction_id")
    alert_id = F.concat_ws("-", F.lit(rule), F.col("source_account"), F.date_format("ws", "yyyyMMddHHmmss"))
    null_dec = F.lit(None).cast("decimal(18,2)")
    if rule == "R2":
        text = F.format_string("Account made %d transactions in a 120s window.", F.col("observed_count"))
    elif rule == "R3":
        text = F.format_string("Account made %d INR transactions totalling INR %s in a 120s window.",
                               F.col("observed_count"), F.col("total").cast("decimal(18,2)").cast("string"))
    else:
        text = F.format_string("Account sent %d transactions to one destination in a 120s window.",
                               F.col("observed_count"))
    return g.select(
        alert_id.alias("alert_id"),
        F.lit(rule).alias("rule_id"),
        F.col("source_account"),
        (F.col("destination_account") if rule == "R4" else F.lit(None).cast("string")).alias("destination_account"),
        trigger_id.alias("trigger_transaction_id"),
        F.lit("PATTERN" if rule == "R4" else "WINDOW").alias("finding_scope"),
        F.col("ws").alias("window_start"),
        F.col("we").alias("window_end"),
        F.col("observed_count"),
        (F.col("total").cast("decimal(18,2)") if rule == "R3" else null_dec).alias("observed_amount_inr"),
        F.lit({"R2": R2_MIN_COUNT, "R3": R3_MIN_COUNT, "R4": R4_MIN_COUNT}[rule]).cast("int").alias("count_threshold"),
        (F.lit(R3_MIN_TOTAL).cast("decimal(18,2)") if rule == "R3" else null_dec).alias("amount_threshold_inr"),
        F.transform("items", lambda x: x["transaction_id"]).alias("supporting_transaction_ids"),
        text.alias("explanation"),
        F.current_timestamp().alias("triggered_at"))


def build_decisions(w, alerts_all):
    """One decision per transaction.

    A transaction belongs to 4 sliding windows; the LAST one to close is the window that starts
    at floor(event_time, 30s). So each transaction is decided exactly once: in the batch where
    that last window closes, i.e. when all 4 of its windows (and their alerts) are final.
    Rule flags use account-level linking: an alert applies to every transaction of that account
    whose event time lies inside the alert window (this is how the ground truth behaves).
    """
    last = (w.where(F.col("event_ts") < F.col("ws") + F.expr("INTERVAL 30 SECONDS"))
              .select("transaction_id", "source_account", "destination_account", "event_ts", "amount", "currency")
              .dropDuplicates(["transaction_id"]))
    a = alerts_all.select("alert_id", "rule_id", F.col("source_account").alias("a_src"), "window_start", "window_end")
    link = (last.join(a, (last["source_account"] == a["a_src"]) &
                      (last["event_ts"] >= a["window_start"]) & (last["event_ts"] < a["window_end"]), "left")
                .groupBy("transaction_id")
                .agg(F.collect_set("rule_id").alias("rules"),
                     F.sort_array(F.collect_set("alert_id")).alias("related_alert_ids")))
    d = last.join(link, "transaction_id", "inner")
    r1 = (F.col("currency") == "INR") & (F.col("amount") >= F.lit(R1_AMOUNT))
    flags = {"r1": r1}
    for n in (2, 3, 4):
        flags[f"r{n}"] = F.array_contains("rules", f"R{n}")
    score = sum(F.when(flags[k], 1).otherwise(0) for k in ("r1", "r2", "r3", "r4"))
    return d.select(
        "transaction_id", "source_account", "destination_account",
        F.col("event_ts").alias("event_timestamp"),
        F.current_timestamp().alias("evaluated_at"),
        "amount", "currency",
        flags["r1"].alias("r1_triggered"), flags["r2"].alias("r2_triggered"),
        flags["r3"].alias("r3_triggered"), flags["r4"].alias("r4_triggered"),
        score.cast("int").alias("r5_risk_score"),
        (score >= 2).alias("r5_block_triggered"),
        F.when(score >= 2, "BLOCK").when(score == 1, "REVIEW").otherwise("APPROVE").alias("decision"),
        "related_alert_ids")


# ----------------------------------------------------------------------------- main
def main():
    args = parse_args()
    topic = args.topic or f"transactions-{args.run}"
    spark = (SparkSession.builder.appName(f"fraud-stream-{args.run}")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    if args.sink == "iceberg":
        sink = IcebergSink(args.catalog, args.db, args.run)
        log(f"sink: Iceberg  decisions={sink.decisions}  alerts={sink.alerts}")
    else:
        sink = ParquetSink(args.out_dir or f"/tmp/fraud_out_{args.run}", args.run)
        log(f"sink: PARQUET TEST SINK at {sink.out}")

    # ---- source
    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", topic)
               .option("startingOffsets", args.starting_offsets)
               .option("failOnDataLoss", "false")
               .load().selectExpr("CAST(value AS STRING) AS value"))
        log(f"source: Kafka {args.bootstrap} topic={topic}")
    else:
        raw = (spark.readStream.option("maxFilesPerTrigger", args.max_files_per_trigger)
               .text(args.input_dir))
        log(f"source: files in {args.input_dir}")

    # ---- parse + watermark + one stateful windowed aggregation
    tx = (raw.select(F.from_json("value", MSG_SCHEMA).alias("m")).select("m.*")
             .where(F.col("transaction_id").isNotNull() & F.col("source_account").isNotNull())
             .withColumn("event_ts", F.to_timestamp("event_timestamp"))
             .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
             .where(F.col("event_ts").isNotNull()))
    members = F.struct("transaction_id", "destination_account", "event_ts", "amount", "currency")
    agg = (tx.withWatermark("event_ts", WATERMARK)
             .groupBy("source_account", F.window("event_ts", WINDOW, SLIDE).alias("w"))
             .agg(F.collect_list(members).alias("members")))

    def process_batch(batch_df, batch_id):
        t0 = time.time()
        session = batch_df.sparkSession          # the cloned session of this micro-batch (see note above)
        w = (batch_df.where(F.col("source_account") != FLUSH_ACCOUNT)
             .select("source_account", F.col("w.start").alias("ws"), F.col("w.end").alias("we"),
                     F.explode("members").alias("m"))
             .select("source_account", "ws", "we", "m.*")
             .dropDuplicates(["source_account", "ws", "transaction_id"])   # duplicates never counted twice
             .persist())
        try:
            n_rows = w.count()
            if n_rows == 0:
                log(f"batch {batch_id}: no closed windows")
                return
            alerts = reduce(lambda a, b: a.unionByName(b), [build_alerts(w, r) for r in ("R2", "R3", "R4")]).persist()
            n_alerts = alerts.count()
            if n_alerts:
                sink.upsert(session, alerts, "alerts", "alert_id")
            decisions = build_decisions(w, sink.read_alerts(session)).persist()
            n_dec = decisions.count()
            if n_dec:
                sink.upsert(session, decisions, "decisions", "transaction_id")
            log(f"batch {batch_id}: closed-window rows={n_rows} alerts={n_alerts} decisions={n_dec} "
                f"({time.time() - t0:.1f}s)")
            alerts.unpersist()
            decisions.unpersist()
        finally:
            w.unpersist()

    ckpt = f"{args.checkpoint_base.rstrip('/')}/{args.run}"
    log(f"checkpoint: {ckpt}")
    query = (agg.writeStream.outputMode("append")
             .foreachBatch(process_batch)
             .option("checkpointLocation", ckpt)
             .trigger(processingTime=args.trigger)
             .start())
    log("streaming query started (Ctrl+C to stop)")

    last_input, seen, batches = time.time(), False, 0
    try:
        while query.isActive:
            query.awaitTermination(5)
            lp = query.lastProgress
            if lp and lp.get("numInputRows", 0) > 0:
                last_input, seen = time.time(), True
                if lp.get("batchId", 0) != batches:
                    batches = lp.get("batchId", 0)
                    wm = lp.get("eventTime", {}).get("watermark", "-")
                    log(f"progress: batch {batches}, input rows so far this batch={lp['numInputRows']}, watermark={wm}")
            if args.idle_exit and seen and time.time() - last_input > args.idle_exit:
                log(f"no new input for {args.idle_exit}s -> stopping")
                query.stop()
                break
    except KeyboardInterrupt:
        log("Ctrl+C received -> stopping query")
        query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
