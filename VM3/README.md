# VM3 (Chiranthan): HDFS + Iceberg + Spark worker + verifier

- ZeroTier IP: 192.168.193.59
- HDFS: hdfs://192.168.193.59:9000 (datanode port 9866)
- Iceberg catalog: iceberg_catalog (Hadoop), warehouse hdfs://192.168.193.59:9000/warehouse, database fraud
- Config: config/iceberg.conf
- Create the tables for a run:
  spark-submit --master local[2] --properties-file config/iceberg.conf scripts/create_tables.py --run run1
- Verifier: verifier/ (not written yet)
- Reports and evidence: reports/
