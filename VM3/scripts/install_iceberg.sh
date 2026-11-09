#!/usr/bin/env bash
set -euo pipefail

ICEBERG_VERSION="1.8.1"
JAR_NAME="iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar"
BASE_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/${ICEBERG_VERSION}"

sudo mkdir -p /opt/iceberg
sudo chown -R "$USER":"$USER" /opt/iceberg
cd /opt/iceberg
rm -f "${JAR_NAME}" "${JAR_NAME}.sha1"

wget --progress=dot:giga "${BASE_URL}/${JAR_NAME}"
wget -q "${BASE_URL}/${JAR_NAME}.sha1"

EXPECTED_SHA1="$(tr -d '[:space:]' < "${JAR_NAME}.sha1")"
ACTUAL_SHA1="$(sha1sum "${JAR_NAME}" | awk '{print $1}')"

if [ "$EXPECTED_SHA1" = "$ACTUAL_SHA1" ]; then
    echo "SHA-1 verification: OK"
else
    echo "SHA-1 verification: FAILED"
    exit 1
fi

rm -f "${JAR_NAME}.sha1"
ls -lh /opt/iceberg/
