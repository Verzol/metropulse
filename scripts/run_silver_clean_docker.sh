#!/bin/bash
# MetroPulse Silver Clean - Docker Execution
# Builds cleaned Silver dataset from compact Silver Core parquet.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "Starting Silver Clean build via Docker Spark..."
echo ""

echo "Copying Silver Clean job to Spark container..."
docker compose cp src/processing/silver_clean_transform.py spark-master:/tmp/
docker compose cp .env spark-master:/tmp/

echo "Executing Spark job..."
echo ""

docker compose exec -T spark-master bash -c "
  cd /tmp

  mkdir -p /tmp/ivy/cache 2>/dev/null || true
  chmod -R 777 /tmp/ivy 2>/dev/null || true

  echo 'Installing Python dependencies...'
  pip install --target /tmp/python-packages --quiet python-dotenv 2>/dev/null || pip install --target /tmp/python-packages python-dotenv
  export PYTHONPATH=/tmp/python-packages:\$PYTHONPATH
  echo 'Dependencies installed'
  echo ''

  /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/ivy \
    --conf spark.sql.session.timeZone=America/New_York \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --driver-memory 4G \
    --executor-memory 8G \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --master spark://spark-master:7077 \
    silver_clean_transform.py
"

echo ""
echo "Silver Clean build completed!"
echo ""
echo "Check data in MinIO:"
echo "   Path: silver/taxi_weather_trips_clean/"
