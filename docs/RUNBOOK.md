# Runbook: baseline + 4 experiments

Naming per run (fresh topic, checkpoint and tables every time):

| Run | Purpose | Producer mode | Topic | Checkpoint | Tables |
|---|---|---|---|---|---|
| run1 | Baseline | `normal` | `transactions-run1` | `/checkpoints/run1` | `*_run1` |
| run2 | Exp 1: kill an executor | `normal` | `transactions-run2` | `/checkpoints/run2` | `*_run2` |
| run3 | Exp 2: kill master + driver, restart from the same checkpoint | `normal` | `transactions-run3` | `/checkpoints/run3` | `*_run3` |
| run4 | Exp 3: out of order < 1 min | `late_lt1m` | `transactions-run4` | `/checkpoints/run4` | `*_run4` |
| run5 | Exp 4: out of order > 1 min | `late_gt1m` | `transactions-run5` | `/checkpoints/run5` | `*_run5` |

Everything is run from the repo root (`cd Distributed-Fraud-Monitoring-System`). Run only ONE experiment at a time.

## A. One-time preparation
* **All VMs:** ZeroTier connected (`ping` the others), `git pull`, `config/cluster.env` has `VM4_IP`.
* **VM1:** Kafka running with `advertised.listeners=PLAINTEXT://192.168.193.163:9092`; `pip install -r VM1/requirements.txt`;
  firewall: `sudo ufw allow in on <zt-interface> to any port 9092`.
* **VM2:** Spark master running on `192.168.193.39:7077`; firewall allows 7077, 7078, 7079, 8080 from the ZeroTier network
  (`sudo ufw allow from 192.168.193.0/24 to any port 7077,7078,7079,8080 proto tcp`).
* **VM3:** `start-dfs.sh`, `hdfs dfsadmin -safemode wait`, Spark worker started
  (`$SPARK_HOME/sbin/start-worker.sh spark://192.168.193.39:7077`). Firewall allows 9000 and 9866 from the ZeroTier network.
* **VM4:** Spark worker started the same way; dashboard venv ready (see `VM4/README.md`).
* **Check:** `http://192.168.193.39:8080` lists **2 workers (ALIVE)**.
* **Optional smoke test of the Iceberg path without Kafka (VM3):** `tools/dry_run.sh run0` should end with `VERDICT: PASS`.

## B. Standard procedure for a run (replace `runN`)
1. **VM3:** `VM3/scripts/create_run.sh runN` (tables + HDFS permissions).
2. **VM1:** `VM1/create_topic.sh runN`.
3. **VM2:** `VM2/run_stream.sh runN --idle-exit 150` (waits for data; stops itself 150 s after the last input).
   Watch lines like `batch 12: closed-window rows=... alerts=... decisions=...`.
4. **VM1:** `VM1/run_producer.sh runN [mode]` (about 12 minutes; progress bar shows messages sent).
5. When the Spark job has stopped on its own: **VM3:** `VM3/verifier/run_verify.sh runN --label "<experiment>"`
   (add `--disorder-log VM1/disorder/disorder_runN.csv` for Exp 3 and 4; copy that file to VM3 through git).
6. **VM4:** `VM4/run_dashboard.sh runN`, take dashboard screenshots.
7. Screenshots: Spark UI (`http://192.168.193.39:8080` and the application UI on port 4040: Executors, Structured Streaming tab),
   HDFS UI (`http://192.168.193.59:9870`), verifier output, producer output.
8. Commit `VM3/reports/verify_runN.txt` and the diffs CSV, and `VM1/disorder/*.csv`.

## C. Baseline (run1)
Standard procedure, mode `normal`. **Expected:** `VERDICT: PASS` (1990 decisions, 129 alerts, 0 duplicates).
Do not start experiments before this passes. If it fails, read `VM3/reports/verify_run1.txt`, fix, and repeat with a **new** run name
(`run1b`), never reuse old topics, checkpoints or tables (or run `VM3/scripts/drop_run.sh run1` first).

## D. Experiment 1: kill and restart an executor (run2)
1. Standard procedure up to step 4; start the producer.
2. When about 40 % of the messages are sent, on **VM3 or VM4** run `jps`, find `CoarseGrainedExecutorBackend`, then
   `kill -9 <pid>`. Screenshot the Spark UI Executors tab before and after.
3. The master starts a replacement executor on a worker, failed tasks are retried, the stream continues. (If both workers have
   executors, killing one is enough; killing the same worker again is the "restart".)
4. Finish and verify. **Expected:** `VERDICT: PASS`, same output as the baseline.
5. **Conclusion to record:** executor loss is handled by Spark task retry and state recovery; no data lost, no duplicates.

## E. Experiment 2: kill master and driver, restart from the same checkpoint (run3)
1. Standard procedure up to step 4; start the producer (it keeps sending, Kafka stores the messages).
2. At about 50 %: on **VM2** `jps`, then `kill -9 <pid of SparkSubmit>` (driver) and `$SPARK_HOME/sbin/stop-master.sh`.
   Note the last `batch N` line and `hdfs dfs -ls /checkpoints/run3/offsets` (screenshots).
3. Start again: `$SPARK_HOME/sbin/start-master.sh`, wait until both workers show ALIVE, then
   `VM2/run_stream.sh run3 --idle-exit 150` with the **same run name** (same checkpoint, topic, tables).
4. Wait for the producer to finish and the job to stop; verify. **Expected:** `VERDICT: PASS`, no duplicates, no missing rows
   (checkpoint restores offsets and window state; MERGE writes are idempotent).
5. If the verifier reports duplicates, the write was not idempotent: check that both tables are written with MERGE (see `docs/DESIGN.md`).

## F. Experiment 3: out of order by less than 1 minute (run4)
Producer mode `late_lt1m`: 15 % of the events are delivered 10-50 s late (event time unchanged). Standard procedure.
**Expected:** `VERDICT: PASS` (measured in our single-machine test: exact match, 295 events delayed), because payments up to 1 minute late still count. Conclusion: the watermark absorbs the disorder.

## G. Experiment 4: out of order by more than 1 minute (run5)
Producer mode `late_gt1m`: 10 % of the events are delivered 300-600 s late. Standard procedure, verifier with `--disorder-log`.
**Expected:** `VERDICT: FAIL` with **missing decisions** (payments whose windows had already closed), a few **mismatched** decisions
(evaluated on fewer windows) and missing or changed alerts. The summary line shows how many differences involve a delayed payment.
Conclusion: data later than the watermark is dropped from closed windows; the watermark trades completeness for bounded state.

*Measured in our single-machine test (same seed 42): 88 decisions missing, 13 decisions evaluated on fewer windows, 3 alerts missing,
34 alerts with changed counts; all 37 differing alerts and 88 of the 101 differing decisions involve a delayed payment. The other 13
decisions are on-time payments whose account-level alert changed because a delayed payment was missing from its window.
Your cluster numbers will be similar but not identical (they depend on micro-batch boundaries).*

## H. Evidence checklist per experiment
- [ ] verifier output (console screenshot) + `VM3/reports/verify_runN.txt`
- [ ] Spark UI screenshots (Executors, Structured Streaming, event time / watermark)
- [ ] producer output (mode, delayed events, duration)
- [ ] dashboard screenshot for the run
- [ ] two sentences: what changed in the output compared with the ground truth, and the conclusion

## I. Troubleshooting
| Symptom | Fix |
|---|---|
| Master UI shows 0 workers | worker not started or cannot reach `192.168.193.39:7077`; check ZeroTier ping, `ufw`, `SPARK_LOCAL_IP` |
| Job hangs at start, executors "Lost" | workers cannot reach the driver: ports 7078/7079 open on VM2, `spark.driver.host` = VM2 ZeroTier IP |
| `cannot connect to Kafka` | VM1 `advertised.listeners`, port 9092, `KAFKA_BOOTSTRAP` |
| HDFS `Name node is in safe mode` | `hdfs dfsadmin -safemode wait` |
| `Table ... not found` | run `VM3/scripts/create_run.sh runN` first |
| Spark does not stop by itself | use `--idle-exit 150` or press Ctrl+C after the verifier-ready message |
| Last decisions missing in the baseline | the flush marker was not sent: let the producer finish completely |
| Need to redo a run | `VM3/scripts/drop_run.sh runN`, recreate the topic with a new name or delete it |
