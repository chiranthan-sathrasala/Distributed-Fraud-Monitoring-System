#!/usr/bin/env bash
# Delete a run's tables and checkpoint (e.g. after a failed attempt).  Asks for confirmation.
#   usage: VM3/scripts/drop_run.sh run1
set -euo pipefail
RUN="${1:?usage: drop_run.sh <run>}"
source "$(dirname "$0")/../../common/env.sh"
read -r -p "Really delete tables and checkpoint of '$RUN'? type yes: " ok
[ "$ok" = "yes" ] || { echo "aborted"; exit 1; }
spark-sql --master "local[2]" "${ICEBERG_CONF[@]}" -e "
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${ICEBERG_DB}.transaction_decisions_${RUN};
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${ICEBERG_DB}.account_risk_alerts_${RUN};" 2>&1 | grep -iE "error|exception" || true
hdfs dfs -rm -r -f "/checkpoints/${RUN}" || true
echo "run $RUN deleted"
