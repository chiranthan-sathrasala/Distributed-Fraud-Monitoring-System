# Distributed Fraud Monitoring System

Real-time fraud monitoring on a 4-VM cluster: **Kafka -> Spark Structured Streaming -> Iceberg on HDFS -> Streamlit
dashboard**, plus a **verifier** that checks the output against the known-correct answer and four **stress experiments**.

```
VM1 (Abdul)       Kafka broker + producer (replays dataset/input/transactions.csv, creates disorder)
VM2 (Anirudh)     Spark master + driver, runs VM2/fraud_stream.py (rules R1-R5, watermark, Iceberg writes)
VM3 (Chiranthan)  HDFS + Iceberg tables + Spark worker + verifier
VM4 (Prajwal)     Spark worker + Streamlit dashboard
```

## Repository layout
| Path | What |
|---|---|
| `config/cluster.env` | all IPs, paths, versions in one place (fill `VM4_IP`) |
| `common/` | shared code: table schemas, replay/disorder logic, `env.sh` (Spark/Iceberg flags) |
| `dataset/` | input CSV and ground truth (copies of the course files) |
| `VM1/` | `producer.py`, `create_topic.sh`, `run_producer.sh` |
| `VM2/` | `fraud_stream.py` (the Spark job), `run_stream.sh` |
| `VM3/` | `scripts/` (create tables, create/drop a run), `verifier/` (`verify.py`, `run_verify.sh`), `reports/` |
| `VM4/` | `dashboard/` (Streamlit app), `run_dashboard.sh` |
| `tools/` | Kafka-free dry run (`dry_run.sh`) and the file-stream generator |
| `tests/` | `local_selftest.sh` (no HDFS/Iceberg needed), `reference_check.py` (pandas check of the rules) |
| `docs/` | `RUNBOOK.md` (step by step, all experiments), `DESIGN.md` (why it works), `REPORT_TEMPLATE.md` |

## Quick start
1. Every VM: clone the repo, edit `config/cluster.env` (`VM4_IP`), optional `config/cluster.local.env` for machine-specific
   overrides such as `ICEBERG_JAR`.
2. VM3: `start-dfs.sh`, start the worker, then `VM3/scripts/create_run.sh run1`.
3. VM1: `VM1/create_topic.sh run1`.
4. VM2: `VM2/run_stream.sh run1 --idle-exit 150`.
5. VM1: `VM1/run_producer.sh run1` (about 12 minutes).
6. VM3 (after the stream stops): `VM3/verifier/run_verify.sh run1` -> **VERDICT: PASS** expected.
7. VM4: `VM4/run_dashboard.sh run1`.

The full procedure with all four experiments, expected results and the evidence checklist is in
**[docs/RUNBOOK.md](docs/RUNBOOK.md)**. Read **[docs/DESIGN.md](docs/DESIGN.md)** before the viva.

## Testing without the cluster
* `tests/local_selftest.sh` runs the whole Spark job on one machine (file source, parquet sink) and verifies the result.
* `tools/dry_run.sh run0` does the same on VM3 with the real HDFS + Iceberg tables, no Kafka and no other VM needed.
* `VM4/run_dashboard.sh demo` opens the dashboard on the ground-truth CSVs.

## Versions (fixed by the course guides)
Java 11, Hadoop 3.3.6, Spark 3.3.4 (Scala 2.12), Python 3.10, Iceberg 1.8.1, Kafka 3.9.x.
