#!/bin/bash
# MetroPulse Bronze Layer Ingestion - Docker Execution
# Runs PySpark job from Spark container (avoids local Java/Scala conflicts)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

cleanup_container_secrets() {
  docker compose exec -T --user root spark-master rm -f /tmp/.env >/dev/null 2>&1 || true
}
trap cleanup_container_secrets EXIT

echo "Starting Bronze Layer Ingestion via Docker Spark..."
echo ""

# Copy files to Spark container using docker compose exec
echo "Copying files to Spark container..."
docker compose cp src/processing/bronze_ingestion.py spark-master:/tmp/
docker compose cp .env spark-master:/tmp/
docker compose exec -T --user root spark-master sh -c 'chown spark:spark /tmp/.env && chmod 600 /tmp/.env'

# Run spark-submit from Docker container
echo "Executing Spark job..."
echo ""

docker compose exec -T spark-master bash -c "
  cd /tmp
  
  # Create ivy cache directory with proper permissions
  mkdir -p /tmp/ivy/cache 2>/dev/null || true
  chmod -R 777 /tmp/ivy 2>/dev/null || true
  
  # Install python packages to /tmp (writable directory)
  echo 'Installing Python dependencies...'
  pip install --target /tmp/python-packages --quiet python-dotenv requests 2>/dev/null || pip install --target /tmp/python-packages python-dotenv requests
  export PYTHONPATH=/tmp/python-packages:\$PYTHONPATH
  echo '✓ Dependencies installed'
  echo ''
  
  # Execute spark-submit with Kafka and Hadoop packages
  /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/ivy \
    --driver-memory 4G \
    --executor-memory 8G \
    --executor-cores 4 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
    --master spark://spark-master:7077 \
    bronze_ingestion.py
"

echo ""
echo "Bronze ingestion completed!"
echo "Default mode uses BRONZE_TRIGGER_AVAILABLE_NOW=true, so the job exits after draining current Kafka offsets."
echo ""
echo "Check data in MinIO:"
echo "   Console: http://localhost:9001 (or your VM external host if exposed)"
echo "   Buckets: bronze/yellow_taxi/, bronze/green_taxi/, bronze/weather/"
