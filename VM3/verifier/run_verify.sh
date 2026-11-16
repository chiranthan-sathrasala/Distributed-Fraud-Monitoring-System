#!/usr/bin/env bash
# Verify a run against the ground truth (run on VM3 after the stream has finished).
#   usage: VM3/verifier/run_verify.sh run1 [--label "Exp 1"] [--disorder-log VM1/disorder/disorder_run5.csv]
set -euo pipefail
RUN="${1:?usage: run_verify.sh <run> [extra verify.py args]}"
shift || true
source "$(dirname "$0")/../../common/env.sh"
"${PYSPARK_PYTHON:-python3}" -c "import pandas" 2>/dev/null || { echo "pandas is missing for ${PYSPARK_PYTHON:-python3}: run  ${PYSPARK_PYTHON:-python3} -m pip install pandas"; exit 2; }
spark-submit --master "local[2]" "${ICEBERG_CONF[@]}" "$REPO_ROOT/VM3/verifier/verify.py" \
  --run "$RUN" --catalog "$ICEBERG_CATALOG" --db "$ICEBERG_DB" "$@" 2>&1 \
  | grep -vE "WARN|SLF4J|NativeCodeLoader|^Setting default|^To adjust" 
exit "${PIPESTATUS[0]}"
