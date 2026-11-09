# VM2 — Spark Processing and Fraud Detection

## Responsibility

VM2 is responsible for the central stream-processing component of the Real-Time Fraud Monitoring System.

**Team Member:** Anirudh M

### Main Responsibilities
- Kafka Consumer
- Apache Spark Master
- Spark Driver
- Fraud Detection Logic
- Transaction Processing using PySpark Structured Streaming
- Writing processed results to Apache Iceberg tables on HDFS

## Environment

| Component | Version |
|---|---|
| Operating System | Ubuntu 24.04.5 LTS |
| Java | OpenJDK 11 |
| Python | 3.10 |
| Hadoop | 3.3.6 |
| Spark | 3.3.4 |
| Kafka | 3.9.2 |
| Scala | 2.12.15 |

## Directory Structure

```text
VM2/
├── README.md
├── reports/
│   └── DAY1_REPORT.md
├── scripts/
│   └── test_spark.py
└── docs/
```

## Day 1 — Environment Setup

Completed:
- Verified Java, Python, Hadoop, Spark and Kafka installations.
- Configured the PySpark environment.
- Executed a local PySpark application successfully.
- Configured Spark environment and default properties.
- Started the Spark Standalone Master.
- Verified Master status through logs, process listing and Web UI.
- Inspected firewall configuration.

Detailed documentation:

`reports/DAY1_REPORT.md`

## Day 2 — Cluster Integration and Streaming Pipeline

Planned:
- Establish ZeroTier connectivity with other team VMs.
- Configure Spark Master and Driver to use the team ZeroTier network.
- Connect VM3 and VM4 Spark Workers.
- Connect to Kafka and consume transaction events.
- Implement transaction parsing and deduplication.
- Implement fraud detection rules R1–R5.
- Configure event-time windows and watermarking.
- Write transaction decisions and account risk alerts to Iceberg tables.
- Configure HDFS checkpoints.
- Validate end-to-end streaming execution.

## Network

| Machine | ZeroTier IP |
|---|---|
| VM1 — Kafka | 192.168.193.163 |
| VM2 — Spark Master | 192.168.193.39 |
| VM3 — HDFS and Iceberg | 192.168.193.59 |
| VM4 — Dashboard | To be confirmed |

Network connectivity and cluster integration are subject to verification.
