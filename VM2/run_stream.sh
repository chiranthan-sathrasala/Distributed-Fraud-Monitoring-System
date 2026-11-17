#!/usr/bin/env bash
# Submit the fraud streaming job to the Spark cluster.   Run on VM2.
#   usage: VM2/run_stream.sh run1 [extra fraud_stream.py args, e.g. --idle-exit 120]
# Starting again with the SAME run name resumes from the same HDFS checkpoint (Experiment 2).
set -euo pipefail
RUN="${1:?usage: run_stream.sh <run> [extra args]}"
shift || true
source "$(dirname "$0")/../common/env.sh"

[ -f "$ICEBERG_JAR" ] || { echo "Iceberg jar not found: $ICEBERG_JAR (set ICEBERG_JAR in config/cluster.local.env)"; exit 1; }

exec spark-submit \
  --master "$SPARK_MASTER_URL" \
  --packages "$KAFKA_PACKAGE" \
  "${ICEBERG_CONF[@]}" \
  --conf "spark.driver.host=${VM2_IP}" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --conf "spark.driver.port=7078" \
  --conf "spark.blockManager.port=7079" \
  --conf "spark.executor.memory=${EXECUTOR_MEMORY:-1g}" \
  --conf "spark.executor.cores=${EXECUTOR_CORES:-1}" \
  --conf "spark.sql.shuffle.partitions=4" \
  "$REPO_ROOT/VM2/fraud_stream.py" \
  --run "$RUN" --source kafka --bootstrap "$KAFKA_BOOTSTRAP" \
  --checkpoint-base "$CHECKPOINT_BASE" --catalog "$ICEBERG_CATALOG" --db "$ICEBERG_DB" \
  "$@"
