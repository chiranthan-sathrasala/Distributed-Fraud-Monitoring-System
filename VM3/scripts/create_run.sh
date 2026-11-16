#!/usr/bin/env bash
# Prepare a run on VM3: create both Iceberg tables and open HDFS permissions.
#   usage: VM3/scripts/create_run.sh run1
set -euo pipefail
RUN="${1:?usage: create_run.sh <run>   e.g. run1}"
source "$(dirname "$0")/../../common/env.sh"
spark-submit --master "local[2]" "${ICEBERG_CONF[@]}" "$REPO_ROOT/VM3/scripts/create_tables.py" --run "$RUN" 2>&1 \
  | grep -E "^\[|^OK|Error|Exception" || true
hdfs dfs -mkdir -p "/checkpoints"
hdfs dfs -chmod -R 777 /warehouse /checkpoints
echo "run $RUN is ready: tables created, topic name will be transactions-$RUN, checkpoint ${CHECKPOINT_BASE}/$RUN"
