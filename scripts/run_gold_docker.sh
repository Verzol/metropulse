#!/bin/bash
# MetroPulse Gold - Docker Execution
# Builds Gold ML-ready datasets from compact Silver Core parquet.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "Starting Gold layer build via Docker Spark..."
echo ""

echo "Copying Gold transform job to Spark container..."
docker compose cp src/processing/gold_transform.py spark-master:/tmp/
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
    --executor-memory 6G \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --master spark://spark-master:7077 \
    gold_transform.py
"

echo ""
echo "Gold layer build completed!"
echo ""
echo "Check data in MinIO:"
echo "   Path: gold/gold_demand_features/"
echo "   Path: gold/gold_fare_tip_features/"
