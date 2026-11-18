#!/usr/bin/env bash
# Start the dashboard on VM4 (reads the Iceberg tables of a run from HDFS on VM3).
#   usage: VM4/run_dashboard.sh [run]          default run: run1
#   open:  http://<VM4-ZeroTier-IP>:8501
# Demo mode without the cluster (ground-truth CSVs):  VM4/run_dashboard.sh demo
set -euo pipefail
source "$(dirname "$0")/../common/env.sh"
RUN="${1:-run1}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$(command -v python3)}"
export ICEBERG_WAREHOUSE ICEBERG_JAR
if [ "$RUN" = "demo" ]; then
  export DASHBOARD_SOURCE=csv DASHBOARD_CSV_DIR="$REPO_ROOT/dataset/ground_truth" DASHBOARD_RUN=demo
else
  export DASHBOARD_SOURCE=iceberg DASHBOARD_RUN="$RUN"
fi
exec streamlit run "$REPO_ROOT/VM4/dashboard/app.py" --server.address 0.0.0.0 --server.port 8501 --server.headless true
