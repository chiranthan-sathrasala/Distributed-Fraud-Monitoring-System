# VM4 (Prajwal): Spark worker + dashboard

## Dashboard
Streamlit app in `VM4/dashboard/` (`app.py` = UI, `data.py` = the only file that touches Spark/files).

Contents: filters (time range, account, rule), KPIs (transactions, alerts, flagged accounts, BLOCK/REVIEW),
charts (alerts by rule, transactions and alerts per minute), tables (flagged accounts, alert details) and an
investigation view (explanation, triggering transaction, supporting transactions, last 20 transactions of the
account). If data cannot be loaded, a clear error box explains why instead of a stack trace.

### One-time setup (Python 3.10)
```bash
python3.10 -m venv ~/venv-dash && source ~/venv-dash/bin/activate
pip install -r VM4/requirements.txt
# Iceberg jar (same one as the other VMs): see VM3/scripts/install_iceberg.sh  ->  /opt/iceberg/
```
If the jar lives somewhere else, put `export ICEBERG_JAR=/path/to/iceberg-spark-runtime-3.3_2.12-1.8.1.jar`
into `config/cluster.local.env`.

### Run
```bash
VM4/run_dashboard.sh demo      # no cluster needed: shows the ground-truth CSVs (good for UI work)
VM4/run_dashboard.sh run1      # reads iceberg_catalog.fraud.*_run1 from HDFS on VM3
```
Open `http://<VM4 ZeroTier IP>:8501`. Tick "Auto-refresh" to watch a live run.

### Spark worker
`$SPARK_HOME/sbin/start-worker.sh spark://192.168.193.39:7077` (set `SPARK_LOCAL_IP` to the VM4 ZeroTier IP first).
