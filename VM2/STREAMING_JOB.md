# VM2: the streaming job

`VM2/fraud_stream.py` is the complete Spark Structured Streaming application (rules R1-R5, event-time windows, watermark,
duplicate handling, Iceberg writes). `VM2/run_stream.sh` submits it to the cluster.

```bash
VM2/run_stream.sh run1 --idle-exit 150      # reads topic transactions-run1, writes *_run1 tables, checkpoint on HDFS
```
* Same run name again = resume from the same checkpoint (Experiment 2).
* Options of the job: `python3 VM2/fraud_stream.py --help` (`--trigger`, `--shuffle-partitions`, `--source files`, ...).
* Requirements on VM2: `config/cluster.env` correct, Iceberg jar present (`ICEBERG_JAR`), internet for `--packages`
  (Kafka connector `spark-sql-kafka-0-10_2.12:3.3.4`; or put the jars in `/opt/spark/jars`), firewall ports 7077/7078/7079.
* How and why it works: `docs/DESIGN.md`.
