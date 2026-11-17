# Distributed Fraud Monitoring System --- Daily Progress Report

**Date:** 17 November 2026\
**Owner:** VM2 --- Anirudh\
**Repository:** `Distributed-Fraud-Monitoring-System`\
**Environment:** Ubuntu 24.04.5 LTS; Python 3.10 virtual environment;
Spark 3.3.4

## 1. Executive Summary

Today's work focused on validating the VM2 Spark Structured Streaming
application locally before proceeding to multi-VM integration.

Completed: - Diagnosed the local Spark test failure caused by Hadoop
resolving local paths through `hdfs://localhost:9000`. - Fixed local
execution using a Spark-specific `fs.defaultFS=file:///` override,
without modifying `core-site.xml`. - Passed the normal-mode streaming
self-test. - Passed the late-event test using 10--50-second delays. -
Ran the beyond-watermark test using 300--600-second delays and observed
missing/mismatched output. - Correlated all 88 missing decisions in that
test with transactions in the disorder log. - Preserved separate output
and verifier reports for each experiment.

**Overall:** Local processing scenarios have been tested. Distributed
Kafka--Spark--HDFS/Iceberg integration, worker connectivity, and
failure-recovery experiments remain pending.

## 2. Environment and Architecture

### Software

  ---------------------------------------------------------------------------------------------
  Component                           Version / Configuration
  ----------------------------------- ---------------------------------------------------------
  OS                                  Ubuntu 24.04.5 LTS

  Java                                OpenJDK 11

  Python                              3.10

  Python environment                  `~/venv-test`

  Spark                               3.3.4

  Scala                               2.12

  Hadoop                              3.3.6

  Kafka                               3.9.2

  Iceberg runtime                     1.8.1, Spark 3.3 / Scala 2.12

  Iceberg JAR                         `/opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar`
  ---------------------------------------------------------------------------------------------

The virtual environment contains PySpark 3.3.4, Py4J, Pandas, and
PyArrow.

### VM roles

  VM    Role                                          ZeroTier IP
  ----- --------------------------------------------- -------------------
  VM1   Kafka broker and transaction producer         `192.168.193.163`
  VM2   Spark Master, Driver, streaming application   `192.168.193.39`
  VM3   HDFS, Iceberg, Spark Worker, verifier         `192.168.193.59`
  VM4   Spark Worker and Streamlit dashboard          Not yet confirmed

Configured endpoints: - Kafka: `192.168.193.163:9092` - Spark Master:
`spark://192.168.193.39:7077` - HDFS: `hdfs://192.168.193.59:9000` -
Spark Web UI: `http://192.168.193.39:8080`

The Spark Master was started earlier and its Web UI was verified
accessible. At the latest recorded check, no Spark Workers had been
confirmed connected.

### Repository state and ownership

The shared project base is present: `common/`, `config/`, `dataset/`,
`docs/`, `tests/`, and `tools/`.

VM2 application files: - `VM2/fraud_stream.py` - `VM2/run_stream.sh` -
`VM2/STREAMING_JOB.md`

These were integrated into `VM2/`. They were previously reported as
untracked; no commit or push was performed during today's tests.

`VM3/` contains Chiranthan's project component and was preserved. No VM3
files were intentionally deleted or overwritten.

## 3. Local Filesystem Configuration Issue

### Failure observed

The first local test failed to create its Parquet output. A direct run
exposed:

``` text
java.net.ConnectException:
Call From ... to localhost:9000 failed
Connection refused
```

### Root cause

`/opt/hadoop/etc/hadoop/core-site.xml` contains:

``` xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
</property>
```

Spark was resolving a local input path using Hadoop's default HDFS
filesystem. No NameNode was listening at `localhost:9000` on VM2.

### Safe correction

The Hadoop configuration was not changed. Local tests were run with a
Spark-specific override:

``` bash
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell"
```

Example:

``` bash
WORK_DIR=/tmp/fraud_selftest \
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell" \
bash tests/local_selftest.sh normal
```

This keeps local testing on `file:///` while preserving the HDFS
configuration needed for distributed execution.

## 4. Application and Experiment Configuration

The local test runs `VM2/fraud_stream.py` with a file-stream source and
Parquet test sink. It does not use Kafka or the multi-VM cluster.

  Setting                Value
  ---------------------- ---------------------------
  Window duration        2 minutes
  Sliding interval       30 seconds
  Event-time watermark   1 minute
  Local master           `local[*]`
  Test sink              Parquet
  Input                  Numbered JSON-lines files

Fraud rules: - **R1:** One INR transaction at or above ₹50,000. -
**R2:** At least 5 transactions from one source account in a window. -
**R3:** At least 2 INR transactions from one source account totalling
more than ₹6,00,000 in a window. - **R4:** At least 3 transactions from
one source account to the same destination in a window.

Decision score: - 0 triggered rules: `APPROVE` - 1 triggered rule:
`REVIEW` - 2 or more triggered rules: `BLOCK`

Ground truth contains 1,990 transaction decisions and 129 account-risk
alerts.

The generator creates numbered JSON-lines files and changes delivery
order while preserving each transaction's original `event_timestamp`. A
synthetic flush marker advances event time so the final windows can
close; the application ignores the marker account in business outputs.

## 5. Experiment 1 --- Normal Delivery

### Command

``` bash
WORK_DIR=/tmp/fraud_selftest \
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell" \
bash tests/local_selftest.sh normal
```

### Observations

-   2,001 events generated, including one flush marker.
-   51 input files.
-   Zero deliberately delayed events.
-   Spark processed the input in micro-batches.
-   The query stopped after 25 seconds without new input.
-   Output was written to the local Parquet test sink.

### Results

  Validation                 Expected   Actual Result
  ------------------------ ---------- -------- --------
  Decisions                     1,990    1,990 PASS
  Alerts                          129      129 PASS
  Missing decisions                 0        0 PASS
  Extra decisions                   0        0 PASS
  Mismatched decisions              0        0 PASS
  Missing alerts                    0        0 PASS
  Extra alerts                      0        0 PASS
  Mismatched alerts                 0        0 PASS
  Duplicate decision IDs            0        0 PASS
  Duplicate alerts/keys             0        0 PASS

Decision distribution:

  Decision     Expected   Actual
  ---------- ---------- --------
  APPROVE         1,659    1,659
  REVIEW            231      231
  BLOCK             100      100

Alerts by rule:

  Rule     Expected   Actual
  ------ ---------- --------
  R2             90       90
  R3             14       14
  R4             25       25

**Verdict: PASS.** Normal-mode local Spark output matched the supplied
ground truth for all verifier checks.

Evidence:

``` text
/tmp/fraud_selftest/reports/verify_selftestnormal.txt
/tmp/fraud_selftest/reports/verify_selftestnormal_diffs.csv
```

## 6. Experiment 2 --- Late Events Within the Watermark (run4)

The replay configuration in `common/replay.py` is:

``` python
"late_lt1m": (0.15, 10, 50)
```

Approximately 15% of events are selected and delayed by 10--50 seconds.
The default random seed is 42, so the schedule is reproducible for the
same input and seed.

### Command

``` bash
WORK_DIR=/tmp/fraud_selftest_run4 \
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell" \
bash tests/local_selftest.sh late_lt1m
```

### Observations

-   2,001 events including the flush marker.
-   51 input files.
-   295 delayed transactions (approximately 14.75%).
-   Delay range: 10--50 seconds.
-   Configured watermark: 60 seconds.

### Results

  Validation               Expected   Actual Result
  ---------------------- ---------- -------- --------
  Decisions                   1,990    1,990 PASS
  Alerts                        129      129 PASS
  Missing decisions               0        0 PASS
  Extra decisions                 0        0 PASS
  Mismatched decisions            0        0 PASS
  Missing alerts                  0        0 PASS
  Extra alerts                    0        0 PASS
  Mismatched alerts               0        0 PASS
  Duplicate records               0        0 PASS

Decision distribution remained APPROVE 1,659, REVIEW 231, BLOCK 100.
Alert distribution remained R2 90, R3 14, R4 25.

The verifier recorded 295 delayed transactions and zero differences
involving delayed transactions.

**Verdict: PASS.**

### Interpretation

For this generated schedule and dataset, Spark produced exactly the same
decisions and alerts as ground truth despite 295 deliberately delayed
events. This demonstrates successful handling of the tested out-of-order
delivery scenario.

This is not a guarantee that every event delayed by less than 60 seconds
will always be accepted. Watermark progression and window boundaries
also determine whether aggregation state remains available.

Evidence:

``` text
/tmp/fraud_selftest_run4/disorder.csv
/tmp/fraud_selftest_run4/reports/verify_selftestlatelt1m.txt
/tmp/fraud_selftest_run4/reports/verify_selftestlatelt1m_diffs.csv
```

## 7. Experiment 3 --- Late Events Beyond the Watermark (run5)

### Configuration discrepancy to resolve

The project planning notes previously described this experiment as using
90--180-second delays. The actual implementation in `common/replay.py`
is:

``` python
"late_gt1m": (0.10, 300, 600)
```

Therefore, today's experiment used **300--600 seconds (5--10 minutes)**,
not 90--180 seconds. The code was not changed during the experiment.
Confirm the intended range with the team before final submission and
keep implementation, report, and presentation consistent.

### Command

``` bash
WORK_DIR=/tmp/fraud_selftest_run5 \
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell" \
bash tests/local_selftest.sh late_gt1m
```

### Observations

-   2,001 events including the flush marker.
-   51 input files.
-   188 delayed transactions (approximately 9.4%).
-   Delay range: 300--600 seconds.
-   Configured watermark: 60 seconds.

### Results

  Validation                 Expected   Actual Result
  ------------------------ ---------- -------- ---------------------
  Decisions                     1,990    1,902 FAIL --- 88 missing
  Alerts                          129      126 FAIL --- 3 missing
  Missing decisions                 0       88 FAIL
  Extra decisions                   0        0 PASS
  Mismatched decisions              0       13 FAIL
  Missing alerts                    0        3 FAIL
  Extra alerts                      0        0 PASS
  Mismatched alerts                 0       34 FAIL
  Duplicate decision IDs            0        0 PASS
  Duplicate alerts/keys             0        0 PASS

Decision distribution:

  Decision     Expected   Actual
  ---------- ---------- --------
  APPROVE         1,659    1,590
  REVIEW            231      215
  BLOCK             100       97

Alerts by rule:

  Rule     Expected   Actual
  ------ ---------- --------
  R2             90       88
  R3             14       14
  R4             25       24

**Verifier verdict: FAIL --- expected differences observed in this
stress experiment.** This is not a ground-truth match; the purpose was
to investigate the effect of events arriving well beyond the configured
watermark.

### Examples of changed alert contents

**R2 --- account `5334683135`, window 11:19:00--11:21:00**

Expected alert: - Observed count: 8 - Trigger transaction: `TXN0000899`

Actual alert: - Observed count: 7 - Trigger transaction: `TXN0000898`

One supporting transaction was absent from the actual aggregation.

**R3 --- account `5389782164`**

Expected: - Observed count: 9 - Total INR amount: ₹7,04,500 - Trigger
transaction: `TXN0001291`

Actual: - Observed count: 8 - Total INR amount: ₹6,43,500 - Trigger
transaction: `TXN0001290`

The actual total remained above the R3 threshold, so an alert still
existed, but its details differed from ground truth.

### Correlation of missing decisions with disorder log

The missing decision IDs from the verifier difference CSV were compared
with transaction IDs in `/tmp/fraud_selftest_run5/disorder.csv`.

  Correlation check                Result
  ------------------------------ --------
  Total missing decisions              88
  Also present in disorder log         88
  Not present in disorder log           0
  Correlation                        100%

Examples:

  Transaction   Original event time (UTC)     Scheduled delay
  ------------- --------------------------- -----------------
  TXN0000060    2026-10-27 08:19:34                 394.6 sec
  TXN0000113    2026-10-27 08:32:20                 397.2 sec
  TXN0000144    2026-10-27 08:39:28                 489.3 sec
  TXN0000148    2026-10-27 08:40:16                 371.4 sec
  TXN0000177    2026-10-27 08:44:45                 599.8 sec

The verifier also reported: - 88 of 101 differing decisions involved
delayed transactions. - All 37 differing alerts involved delayed
transactions.

**Interpretation:** This correlation strongly supports the conclusion
that delayed delivery was associated with the missing decisions. The
disorder log records scheduled delivery delays, not Spark's exact
watermark value at each processing instant. Therefore, it is supporting
evidence, not a direct measurement of the instant each event was
dropped.

Evidence:

``` text
/tmp/fraud_selftest_run5/disorder.csv
/tmp/fraud_selftest_run5/reports/verify_selftestlategt1m.txt
/tmp/fraud_selftest_run5/reports/verify_selftestlategt1m_diffs.csv
```

## 8. Consolidated Experiment Comparison

  Metric                   Normal   Late \<1 min                    Late \>1 min
  ---------------------- -------- -------------- -------------------------------
  Delayed transactions          0            295                             188
  Delay range                None     10--50 sec                    300--600 sec
  Decisions generated       1,990          1,990                           1,902
  Alerts generated            129            129                             126
  Missing decisions             0              0                              88
  Mismatched decisions          0              0                              13
  Missing alerts                0              0                               3
  Mismatched alerts             0              0                              34
  Duplicate records             0              0                               0
  Verdict                    PASS           PASS   FAIL --- expected differences

### Overall conclusion

1.  Normal delivery produced the expected transaction decisions and
    account-risk alerts.
2.  The tested 10--50-second out-of-order delivery scenario produced no
    differences from ground truth.
3.  The tested 300--600-second delayed delivery scenario produced
    missing and mismatched decisions and alerts.
4.  All 88 missing decisions were present in the disorder log.
5.  These local experiments demonstrate the practical effect of bounded
    event-time lateness on stateful streaming aggregation.

A verifier FAIL in run5 is the expected experimental observation. It
should not be described as a successful ground-truth match.

## 9. Concepts Reviewed

### Structured Streaming micro-batches

The application processes incoming data as successive micro-batches.
Execution logs showed batch IDs and the number of closed-window rows,
alerts, and decisions.

### Event time vs processing time

-   **Event time:** When the transaction actually occurred.
-   **Delivery/processing time:** When it reaches and is processed by
    the streaming application.

The replay generator preserves the original event timestamp and changes
delivery order.

### Watermark

A watermark helps Spark manage state for event-time operations while
tolerating bounded lateness. It helps determine when old windows can be
finalized and state can eventually be removed.

It is not a universal per-record timer: window boundaries and watermark
progression matter.

### Sliding windows

The application uses 2-minute windows advancing every 30 seconds.
Adjacent windows overlap, so one event can contribute to multiple window
aggregations.

### Parquet vs Iceberg

Today's local tests used a Parquet test sink. They did not validate
writes to the project's Iceberg tables on HDFS.

### Ground-truth verification

The verifier compares actual output with expected CSV ground truth. It
checks counts, missing/extra records, mismatched values, and duplicates.

## 10. Current Project Status

  ----------------------------------------------------------------------------
  Workstream                   Status                  Notes
  ---------------------------- ----------------------- -----------------------
  Spark                        COMPLETE                Spark 3.3.4 environment
  installation/configuration                           prepared
  on VM2                                               

  Spark Master startup         COMPLETE                Master process started

  Spark Web UI                 VERIFIED                Port 8080 previously
                                                       returned HTTP 200

  Shared project base          INTEGRATED              Shared directories
                                                       present

  VM2 streaming files          INTEGRATED LOCALLY      Three files present;
                                                       previously reported
                                                       untracked

  Iceberg runtime JAR          INSTALLED               Version 1.8.1 runtime

  Python environment           COMPLETE                Python 3.10 with
                                                       PySpark and
                                                       dependencies

  Ground-truth reference check PASS                    129 alerts and 1,990
                                                       decisions matched

  Local normal streaming test  PASS                    1,990 decisions, 129
                                                       alerts

  Late-under-watermark test    PASS                    295 delayed events,
                                                       zero differences

  Late-beyond-watermark test   EXPECTED DIFFERENCES    88 missing decisions, 3
                                                       missing alerts,
                                                       mismatches

  Missing-decision correlation COMPLETE                88/88 missing decisions
                                                       appear in disorder log

  VM1 Kafka connectivity from  PENDING                 Must verify
  VM2                                                  

  VM3 HDFS connectivity from   PENDING                 Must verify
  VM2                                                  

  Spark Worker registration    PENDING                 No connected worker
                                                       confirmed in latest
                                                       check

  VM4 worker/dashboard         PENDING                 VM4 IP needs
  integration                                          confirmation

  Kafka-to-Spark integration   PENDING                 Not validated by local
                                                       file-stream tests

  HDFS/Iceberg writes          PENDING                 Not validated by local
                                                       Parquet tests

  Distributed failure recovery PENDING                 Executor/driver failure
                                                       tests remain
  ----------------------------------------------------------------------------

## 11. Next Steps

1.  Verify ZeroTier connectivity from VM2 to VM1 and VM3, including
    required service ports.
2.  Verify VM2 can access the HDFS NameNode on VM3 and list the
    configured HDFS paths.
3.  Register Spark Workers and confirm they appear in the Spark Master
    Web UI.
4.  Verify the Kafka broker on VM1 is reachable and the required topic
    exists.
5.  Run the streaming application against the configured Spark Master
    and Kafka source.
6.  Validate writes to the run-specific Iceberg tables in the configured
    catalog and warehouse.
7.  Run the verifier against the distributed Iceberg output.
8.  Perform executor failure and Master/Driver restart experiments using
    the documented checkpoint strategy.
9.  Reconcile the beyond-watermark delay discrepancy (planning notes
    90--180 seconds vs implementation 300--600 seconds) with the team.

## 12. Reproduction Commands and Evidence

### Normal

``` bash
WORK_DIR=/tmp/fraud_selftest \
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell" \
bash tests/local_selftest.sh normal
```

### Late under watermark

``` bash
WORK_DIR=/tmp/fraud_selftest_run4 \
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell" \
bash tests/local_selftest.sh late_lt1m
```

### Late beyond watermark

``` bash
WORK_DIR=/tmp/fraud_selftest_run5 \
PYSPARK_SUBMIT_ARGS="--master local[*] --conf spark.hadoop.fs.defaultFS=file:/// pyspark-shell" \
bash tests/local_selftest.sh late_gt1m
```

### Reference validation

``` bash
python tests/reference_check.py
```

Previously observed:

``` text
alerts: mine 129, ground truth 129, differences 0
decisions: 1990 checked, mismatches 0
RESULT: MATCH - rules reproduce the ground truth
```

Evidence directories:

``` text
/tmp/fraud_selftest/
/tmp/fraud_selftest_run4/
/tmp/fraud_selftest_run5/
```

Each contains generated input, disorder logs where applicable, Parquet
output, and verifier reports.

## 13. Final Status Summary

Today's main achievement is the local validation of the VM2 streaming
application across normal, moderately out-of-order, and heavily delayed
event delivery scenarios.

The normal and within-watermark experiments matched ground truth
exactly. The beyond-watermark experiment produced incomplete and
mismatched results, and all missing decisions were correlated with
deliberately delayed transactions.

**Next milestone:** Validate the actual multi-VM Kafka → Spark →
HDFS/Iceberg pipeline. Local Parquet test success is a useful validation
step, but it is not a substitute for distributed integration testing.
