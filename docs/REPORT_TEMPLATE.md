# Report template

## 1. System overview
Architecture (4 VMs), data flow, versions (Java 11, Hadoop 3.3.6, Spark 3.3.4, Iceberg 1.8.1, Kafka 3.9). Link `docs/DESIGN.md` choices:
single windowed aggregation, foreachBatch, idempotent MERGE, flush marker, speed 40x.

## 2. Baseline (run1)
Verifier verdict, counts (1990 decisions, 129 alerts), duration, screenshots (Spark UI, dashboard, verifier).

## 3. Experiments
For each of the four experiments:
| | |
|---|---|
| What we did | exact action and time (e.g. `kill -9` of the executor at batch N) |
| What changed in the output (verifier vs ground truth) | counts: missing / extra / mismatched / duplicates, example differences |
| Why it happened | Spark mechanism (task retry, checkpoint, watermark) |
| Conclusion | worked / did not work / partly, in one sentence |
| Evidence | screenshot file names |

Expected outcomes: Exp 1 PASS, Exp 2 PASS (no duplicates), Exp 3 PASS, Exp 4 FAIL (late events dropped).

## 4. Verifier design
Checks listed in `VM3/verifier/verify.py`, ignored fields, exit codes.

## 5. Dashboard
Screenshots of filters, KPIs, charts, tables, investigation view, error message.

## 6. Limitations and lessons
Decision latency (about 3 event-minutes), watermark trade-off, single-node HDFS, Spark 3.3 limits.
