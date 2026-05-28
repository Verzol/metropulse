#!/bin/bash
# MetroPulse PostgreSQL fare/tip Quality Check - Docker Execution
# Validates published trip-level ML features against Gold Parquet in MinIO.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

cleanup_container_secrets() {
  docker compose exec -T --user root spark-master rm -f /tmp/.env >/dev/null 2>&1 || true
}
trap cleanup_container_secrets EXIT

docker compose cp src/quality/postgres_fare_tip_quality_check.py spark-master:/tmp/
docker compose cp .env spark-master:/tmp/
docker compose exec -T --user root spark-master sh -c 'chown spark:spark /tmp/.env && chmod 600 /tmp/.env'

if docker compose exec -T spark-master bash -c "
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
    postgres_fare_tip_quality_check.py
"; then
  docker compose exec -T warehouse-postgres sh -c \
    'psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --command "UPDATE audit.publish_run_history SET status = '\''passed'\'', completed_at = CURRENT_TIMESTAMP, details = '\''Validated fare/tip features against MinIO Gold source.'\'' WHERE publish_run_id = (SELECT publish_run_id FROM audit.publish_run_history WHERE target_table = '\''ml.gold_fare_tip_features'\'' AND status = '\''started'\'' ORDER BY publish_run_id DESC LIMIT 1);"'
  echo "PostgreSQL fare/tip validation completed successfully."
else
  docker compose exec -T warehouse-postgres sh -c \
    'psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --command "UPDATE audit.publish_run_history SET status = '\''failed'\'', completed_at = CURRENT_TIMESTAMP, details = '\''Fare/tip source-target validation failed.'\'' WHERE publish_run_id = (SELECT publish_run_id FROM audit.publish_run_history WHERE target_table = '\''ml.gold_fare_tip_features'\'' AND status = '\''started'\'' ORDER BY publish_run_id DESC LIMIT 1);"'
  echo "PostgreSQL fare/tip validation failed."
  exit 1
fi
