#!/bin/bash
# MetroPulse Bronze Layer Ingestion - Docker Execution
# Runs PySpark job from Spark container (avoids local Java/Scala conflicts)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "Starting Bronze Layer Ingestion via Docker Spark..."
echo ""

# Copy files to Spark container using docker compose exec
echo "Copying files to Spark container..."
docker compose cp src/processing/bronze_ingestion.py spark-master:/tmp/
docker compose cp .env spark-master:/tmp/

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
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    --master spark://spark-master:7077 \
    bronze_ingestion.py
"

echo ""
echo "Bronze ingestion completed!"
echo ""
echo "Check data in MinIO:"
echo "   http://34.21.193.160:9001  (or http://localhost:9001 with SSH tunnel)"
echo "   Buckets: bronze/yellow_taxi/, bronze/green_taxi/, bronze/weather/"

