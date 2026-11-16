#!/usr/bin/env bash
# Run the complete job on VM3 WITHOUT Kafka and WITHOUT the cluster, but with the real HDFS + Iceberg tables.
# Proves the Spark logic, the Iceberg MERGE writes, the HDFS checkpoint and the verifier together.
#   usage: tools/dry_run.sh run0 [normal|late_lt1m|late_gt1m]
set -euo pipefail
RUN="${1:?usage: dry_run.sh <run> [mode]   e.g. run0}"
MODE="${2:-normal}"
source "$(dirname "$0")/../common/env.sh"
"$REPO_ROOT/VM3/scripts/create_run.sh" "$RUN"
IN="/tmp/fraud_files_$RUN"
python3 "$REPO_ROOT/tools/make_file_stream.py" --out "$IN" --mode "$MODE" --events-per-file 20
spark-submit --master "local[2]" "${ICEBERG_CONF[@]}" "$REPO_ROOT/VM2/fraud_stream.py" \
  --run "$RUN" --source files --input-dir "$IN" --checkpoint-base "$CHECKPOINT_BASE" \
  --catalog "$ICEBERG_CATALOG" --db "$ICEBERG_DB" --trigger "2 seconds" --idle-exit 60 \
  2>&1 | grep -E "^\[[0-9:]+\] (batch|no new|sink|source|checkpoint)" || true
"$REPO_ROOT/VM3/verifier/run_verify.sh" "$RUN" --label "dry run $MODE"
