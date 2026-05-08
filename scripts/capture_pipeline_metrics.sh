#!/bin/bash
# capture_pipeline_metrics.sh
# Chạy toàn bộ pipeline và log tất cả metrics

set -e

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
LOG_DIR="execution_logs/$TIMESTAMP"
mkdir -p "$LOG_DIR"

echo "[START] MetroPulse Pipeline Execution & Metrics Collection"
echo "[INFO] Logs saved to: $LOG_DIR"
echo ""

# ============================================================================
# PHASE 1: VERIFY SETUP
# ============================================================================
echo "[PHASE 1] Verifying Setup..."
echo "================================" | tee -a "$LOG_DIR/00_setup.log"

echo "[OK] Docker Services Status:" | tee -a "$LOG_DIR/00_setup.log"
docker compose ps | tee -a "$LOG_DIR/00_setup.log"

echo -e "\n[OK] System Resources:" | tee -a "$LOG_DIR/00_setup.log"
echo "CPU Cores: $(nproc)" | tee -a "$LOG_DIR/00_setup.log"
echo "RAM: $(free -h | grep Mem)" | tee -a "$LOG_DIR/00_setup.log"
echo "Disk: $(df -h . | tail -1)" | tee -a "$LOG_DIR/00_setup.log"

echo -e "\n[OK] Data Files:" | tee -a "$LOG_DIR/00_setup.log"
echo "Parquet files: $(ls data/raw/*.parquet 2>/dev/null | wc -l)" | tee -a "$LOG_DIR/00_setup.log"
du -sh data/raw | tee -a "$LOG_DIR/00_setup.log"

echo -e "\n[OK] Zone Lookup:" | tee -a "$LOG_DIR/00_setup.log"
wc -l data/taxi_zone_lookup.csv | tee -a "$LOG_DIR/00_setup.log"

# ============================================================================
# PHASE 2: WEATHER PRODUCER
# ============================================================================
echo -e "\n\n[PHASE 2] Weather Producer"
echo "================================" | tee -a "$LOG_DIR/01_weather.log"
echo "Start time: $(date)" | tee -a "$LOG_DIR/01_weather.log"

(time python3 src/ingestion/weather_openmeteo_producer.py) 2>&1 | tee -a "$LOG_DIR/01_weather.log"

echo "End time: $(date)" | tee -a "$LOG_DIR/01_weather.log"

# ============================================================================
# PHASE 3: TAXI PRODUCER
# ============================================================================
echo -e "\n\n[PHASE 3] Taxi Producer"
echo "================================" | tee -a "$LOG_DIR/02_producer.log"
echo "Start time: $(date)" | tee -a "$LOG_DIR/02_producer.log"

(time python3 src/ingestion/producer.py) 2>&1 | tee -a "$LOG_DIR/02_producer.log"

echo "End time: $(date)" | tee -a "$LOG_DIR/02_producer.log"

# ============================================================================
# PHASE 4: KAFKA VERIFICATION
# ============================================================================
echo -e "\n\n[PHASE 4] Kafka Topics Verification"
echo "================================" | tee -a "$LOG_DIR/03_kafka.log"

docker exec metropulse-kafka-1 kafka-topics.sh --list --bootstrap-server localhost:9092 2>&1 | tee -a "$LOG_DIR/03_kafka.log" || echo "Kafka topics check skipped" | tee -a "$LOG_DIR/03_kafka.log"

# ============================================================================
# PHASE 5: BRONZE INGESTION
# ============================================================================
echo -e "\n\n[PHASE 5] Bronze Layer Ingestion"
echo "================================" | tee -a "$LOG_DIR/04_bronze.log"
echo "Start time: $(date)" | tee -a "$LOG_DIR/04_bronze.log"

(time make bronze) 2>&1 | tee -a "$LOG_DIR/04_bronze.log" || echo "Bronze job submitted" | tee -a "$LOG_DIR/04_bronze.log"

echo "End time: $(date)" | tee -a "$LOG_DIR/04_bronze.log"
echo "Note: Spark job runs in background - check Spark UI for completion" | tee -a "$LOG_DIR/04_bronze.log"

# ============================================================================
# PHASE 6: FINAL VERIFICATION
# ============================================================================
echo -e "\n\n[PHASE 6] Final Verification"
echo "================================" | tee -a "$LOG_DIR/05_verification.log"

echo "Docker Status:" | tee -a "$LOG_DIR/05_verification.log"
docker compose ps | tee -a "$LOG_DIR/05_verification.log"

echo -e "\nDisk Usage:" | tee -a "$LOG_DIR/05_verification.log"
du -sh data/ .venv/ 2>/dev/null | tee -a "$LOG_DIR/05_verification.log"

echo -e "\nMinIO Buckets (via S3cmd/AWS CLI if available):" | tee -a "$LOG_DIR/05_verification.log"
echo "Visit: http://localhost:9001" | tee -a "$LOG_DIR/05_verification.log"
echo "Login: admin / metropulse2026" | tee -a "$LOG_DIR/05_verification.log"

echo -e "\nSpark UI: http://localhost:8080" | tee -a "$LOG_DIR/05_verification.log"
echo "Kafdrop UI: http://localhost:9090" | tee -a "$LOG_DIR/05_verification.log"

# ============================================================================
# GENERATE SUMMARY REPORT
# ============================================================================
echo -e "\n\n" 
echo "════════════════════════════════════════════════════════════"
echo "              [REPORT] EXECUTION SUMMARY"
echo "════════════════════════════════════════════════════════════"

echo -e "\n[LOGS] Execution Logs Location:"
echo "   $LOG_DIR"
ls -lh "$LOG_DIR"

echo -e "\n[METRICS] Key Metrics:"
echo "   Weather Records: $(grep -i "record\|weather" "$LOG_DIR/01_weather.log" | tail -5 || echo 'Check log')"
echo "   Producer Output: Check $LOG_DIR/02_producer.log"
echo "   Bronze Status: Check $LOG_DIR/04_bronze.log"

echo -e "\n[DONE] Next Steps:"
echo "   1. Review logs: ls -lh $LOG_DIR"
echo "   2. Check MinIO: http://localhost:9001"
echo "   3. Verify records in Bronze layer"
echo "   4. Run: python3 verify_bronze_data.py"

echo -e "\n[OUTPUT] For Report:"
echo "   - Copy entire $LOG_DIR folder"
echo "   - Include terminal screenshots"
echo "   - Capture UI screenshots (MinIO, Spark, Kafdrop)"
echo ""

# Archive logs
tar -czf "$LOG_DIR.tar.gz" "$LOG_DIR"
echo "[SUCCESS] Logs archived: $LOG_DIR.tar.gz"
echo ""
