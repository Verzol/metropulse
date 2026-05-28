#!/bin/bash
# MetroPulse Gold fare/tip PostgreSQL Publisher - Docker Execution
# Stages the trip-level ML table with conservative JDBC parallelism.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

cleanup_container_secrets() {
  docker compose exec -T --user root spark-master rm -f /tmp/.env >/dev/null 2>&1 || true
}
trap cleanup_container_secrets EXIT

echo "Starting Gold fare/tip publication to PostgreSQL via Docker Spark..."

docker compose cp src/serving/publish_gold_fare_tip_to_postgres.py spark-master:/tmp/
docker compose cp .env spark-master:/tmp/
docker compose exec -T --user root spark-master sh -c 'chown spark:spark /tmp/.env && chmod 600 /tmp/.env'

docker compose exec -T \
  -e WAREHOUSE_FARE_TIP_REUSE_STAGING="${WAREHOUSE_FARE_TIP_REUSE_STAGING:-false}" \
  spark-master bash -c "
  cd /tmp
  mkdir -p /tmp/ivy/cache 2>/dev/null || true
  chmod -R 777 /tmp/ivy 2>/dev/null || true
  pip install --target /tmp/python-packages --quiet python-dotenv 2>/dev/null || pip install --target /tmp/python-packages python-dotenv
  export PYTHONPATH=/tmp/python-packages:\$PYTHONPATH

  /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/ivy \
    --conf spark.sql.session.timeZone=America/New_York \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --driver-memory 4G \
    --executor-memory 6G \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.5 \
    --master spark://spark-master:7077 \
    publish_gold_fare_tip_to_postgres.py
"

echo "Promoting validated fare/tip staging table into ml.gold_fare_tip_features..."
docker compose exec -T warehouse-postgres sh -c \
  'psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
  < sql/postgres/promote_gold_fare_tip_features.sql

echo "Gold fare/tip features promoted; PostgreSQL validation is required next."
