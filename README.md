# MetroPulse

An intelligent data platform that integrates NYC taxi records with environmental factors to forecast urban mobility demand and optimize fleet operations.

---

## Architecture

**Medallion Architecture (3-layer data lakehouse):**
- **Bronze Layer**: Raw data from Kafka → MinIO (parquet format)
- **Silver Layer**: Cleaned & validated data (data quality checks)
- **Gold Layer**: Aggregated & business-ready data for analytics/ML

**Tech Stack:**
- **Kafka**: Streaming data ingestion (wurstmeister/kafka)
- **Spark**: Distributed data processing (Apache Spark 3.5.0)
- **MinIO**: S3-compatible object storage
- **Docker**: Containerization for all services

---

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Python 3.8+ with venv
- GCP VM or local machine with 4GB+ RAM

### Setup Instructions

**1. Clone the repository**
```bash
git clone <your-repo-url>
cd metropulse
```

**2. Create `.env` file from template**
```bash
cp .env.example .env
```

**3. Setup Python environment**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**4. Download NYC Taxi data**
```bash
chmod +x download_data.sh
./download_data.sh
# This downloads ~50GB of parquet files (Yellow & Green taxi, 2023-2024)
```

**5. Start Docker services**
```bash
docker compose up -d
sleep 30  # Wait for services to fully initialize
docker compose ps  # Verify all services are running
```

### Running the Pipeline

**Terminal 1: Start Producer (ingest data to Kafka)**
```bash
source .venv/bin/activate
python3 src/ingestion/producer.py
# Streams all parquet files → Kafka topics
# yellow_taxi_stream for Yellow taxi data
# green_taxi_stream for Green taxi data
```

**Terminal 2: Start Spark Bronze Ingestion (Kafka → MinIO)**
```bash
source .venv/bin/activate
spark-submit src/processing/bronze_ingestion.py
# Reads from Kafka topics
# Writes raw JSON data to MinIO Bronze layer
# Partitioned by taxi type (yellow_taxi, green_taxi)
```

---

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafdrop** (Kafka UI) | http://34.21.193.160:9090 | - |
| **MinIO Console** (Object Storage) | http://34.21.193.160:9001 | admin / metropulse2026 |
| **Spark Master** (Cluster UI) | http://34.21.193.160:8080 | - |

---

## Data Persistence

**Important:** MinIO data is stored in Docker named volumes (`minio_data`).

- `docker compose stop` → Data is **preserved**
- `docker compose down` → Data is **preserved** 
- `docker compose down -v` → Data is **deleted** (destructive!)

**Never use `-v` flag unless you want to delete all data!**

---

## Configuration

### Credentials (stored in `.env`)
```
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=metropulse2026
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### Kafka Configuration
- **Listeners**: 
  - `9092` for host connections (Producer)
  - `29092` for internal Docker connections (Kafdrop, Spark)
- **Auto topic creation**: Enabled
- **Retention**: 7 days (for fault tolerance)

---

## Project Structure
```
metropulse/
├── src/
│   ├── ingestion/
│   │   └── producer.py          # Kafka Producer (parquet → Kafka)
│   └── processing/
│       └── bronze_ingestion.py  # Spark streaming (Kafka → MinIO)
├── data/
│   └── raw/                     # Downloaded NYC taxi parquet files
├── docs/
│   ├── INFRASTRUCTURE_SETUP.md  # VM setup guide
│   └── CONTRIBUTING.MD          # Team development guidelines
├── docker-compose.yml           # All services configuration
├── requirements.txt             # Python dependencies
└── download_data.sh             # Download NYC taxi data
```

---

## License

MetroPulse - Big Data Capstone Project (UET-2026)
