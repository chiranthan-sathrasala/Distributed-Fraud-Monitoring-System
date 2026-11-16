#!/usr/bin/env bash
# Quick connectivity and tooling check. Run on any VM:  tools/preflight.sh
source "$(dirname "$0")/../common/env.sh"
ok=0; bad=0
check() { # name, command...
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "  [ OK ] $name"; ok=$((ok+1)); else echo "  [FAIL] $name"; bad=$((bad+1)); fi
}
port() { timeout 4 bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null; }
echo "Network (ZeroTier)"
check "VM1 Kafka        ${VM1_IP}:9092" port "$VM1_IP" 9092
check "VM2 Spark master ${VM2_IP}:7077" port "$VM2_IP" 7077
check "VM3 HDFS         ${VM3_IP}:9000" port "$VM3_IP" 9000
check "VM3 datanode     ${VM3_IP}:9866" port "$VM3_IP" 9866
echo "Tools on this machine"
check "java"               command -v java
check "spark-submit"       command -v spark-submit
check "Iceberg jar         ${ICEBERG_JAR}" test -f "$ICEBERG_JAR"
check "python pandas (verifier/dashboard)" "${PYSPARK_PYTHON:-python3}" -c "import pandas"
check "python kafka-python (VM1 only)" python3 -c "import kafka"
check "hdfs client (VM3 only)" command -v hdfs
echo "Result: $ok ok, $bad failed (failures only matter for the roles of this VM)"
