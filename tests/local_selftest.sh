#!/usr/bin/env bash
# Whole-pipeline self test on ONE machine: no Kafka, no HDFS, no Iceberg, no cluster.
# Streams the dataset from files into the Spark job (parquet test sink) and runs the verifier.
#   usage: tests/local_selftest.sh [normal|late_lt1m|late_gt1m]      (needs: pyspark 3.3.x, pandas, pyarrow, Java 11/17)
set -euo pipefail
MODE="${1:-normal}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TAG="selftest_$MODE"; TAG="${TAG//_/}"
WORK="${WORK_DIR:-/tmp/fraud_selftest}"
rm -rf "$WORK"; mkdir -p "$WORK"
python3 "$ROOT/tools/make_file_stream.py" --out "$WORK/in" --mode "$MODE" --events-per-file 40 --disorder-log "$WORK/disorder.csv"
python3 "$ROOT/VM2/fraud_stream.py" --run "$TAG" --source files --input-dir "$WORK/in" \
  --checkpoint-base "$WORK/ckpt" --sink parquet --out-dir "$WORK/out" --trigger "1 seconds" --idle-exit 25 \
  2>&1 | grep -E "^\[[0-9:]+\] (batch|no new|sink|source)" || true
python3 "$ROOT/VM3/verifier/verify.py" --run "$TAG" --source parquet --parquet-dir "$WORK/out" \
  --report-dir "$WORK/reports" --label "local self test ($MODE)" --disorder-log "$WORK/disorder.csv"
