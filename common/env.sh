# Source this from any script:  source "$(dirname "$0")/../common/env.sh"
# Loads config/cluster.env (+ optional config/cluster.local.env) and builds the Spark flags
# that every Iceberg-aware spark-submit needs.
_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$_ENV_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$REPO_ROOT/config/cluster.env"
[ -f "$REPO_ROOT/config/cluster.local.env" ] && source "$REPO_ROOT/config/cluster.local.env"

ICEBERG_CONF=(
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE}"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.cache-enabled=false"
  --conf "spark.sql.session.timeZone=UTC"
  --jars "${ICEBERG_JAR}"
)
export REPO_ROOT ICEBERG_CATALOG ICEBERG_DB CHECKPOINT_BASE KAFKA_BOOTSTRAP
