# VM3 (Chiranthan): HDFS + Iceberg + Spark worker + verifier

* ZeroTier IP `192.168.193.59`, HDFS `hdfs://192.168.193.59:9000` (datanode 9866, UI 9870)
* Iceberg catalog `iceberg_catalog` (Hadoop), warehouse `hdfs://192.168.193.59:9000/warehouse`, database `fraud`

## Scripts
| Script | Purpose |
|---|---|
| `scripts/create_run.sh runN` | create both Iceberg tables for a run and open HDFS permissions |
| `scripts/drop_run.sh runN` | delete a run's tables and checkpoint (asks for confirmation) |
| `scripts/create_tables.py` | the table DDL (schemas from `common/schemas.py`) |
| `scripts/stream_smoke_test.py` | small streaming write test to Iceberg with an HDFS checkpoint |
| `verifier/run_verify.sh runN` | **verifier**: compares a run with the ground truth, prints PASS/FAIL |
| `reports/` | verifier reports and difference CSVs (commit them as evidence) |

## Verifier
```bash
VM3/verifier/run_verify.sh run1 --label "baseline"
VM3/verifier/run_verify.sh run5 --label "Exp 4" --disorder-log VM1/disorder/disorder_run5.csv
```
Checks: tables not empty, no duplicates, counts equal the ground truth (1990 decisions, 129 alerts), every decision and alert
equal. Ignored fields: `evaluated_at`, `triggered_at`, `explanation`. Exit code 0 = PASS, 1 = FAIL, 2 = could not load.
It prints a WhatsApp-ready plain-text summary and writes `reports/verify_<run>.txt` and `reports/verify_<run>_diffs.csv`.

## Each time VM3 is started
`start-dfs.sh && hdfs dfsadmin -safemode wait`, then the Spark worker: `$SPARK_HOME/sbin/start-worker.sh spark://192.168.193.39:7077`.
