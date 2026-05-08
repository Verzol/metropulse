# METROPULSE - MAKEFILE GUIDE

## Các Lệnh

### Setup

| Lệnh | Mục đích |
|------|---------|
| `make venv` | Tạo Python virtual environment |
| `make install` | Cài packages (pip + requirements.txt) |

### Docker Services

| Lệnh | Mục đích |
|------|---------|
| `make start` | Khởi động tất cả services (Kafka, MinIO, Spark) |
| `make stop` | Dừng services (giữ data) |
| `make restart` | Restart |
| `make status` | Kiểm tra trạng thái |
| `make logs` | Xem logs realtime |

### Data Pipeline

| Lệnh | Mục đích |
|------|---------|
| `make weather` | Stream weather data (Open-Meteo → Kafka) |
| `make producer` | Stream taxi data (Parquet → Kafka) |
| `make bronze` | Ingest Kafka → MinIO (Spark Streaming) |

### Cleanup

| Lệnh | Mục đích |
|------|---------|
| `make clean` | Dừng services (giữ data) |
| `make clean-all` | Xóa tất cả services + data ⚠️ |

## Workflow

### Lần Đầu

```bash
make install    # Setup Python + packages
make start      # Start Docker services
make status     # Kiểm tra
```

### Chạy Pipeline

**Terminal 1 - Weather data:**
```bash
source .venv/bin/activate
make weather
```

**Terminal 2 - Taxi data:**
```bash
source .venv/bin/activate
make producer
```

**Terminal 3 - Bronze ingestion:**
```bash
make bronze
```

### Monitoring

```bash
make logs           # Terminal logs
make status         # Service status
```

Browser:
- Kafka UI: http://localhost:9090
- MinIO: http://localhost:9001
- Spark: http://localhost:8080

## Data Streams

| Stream | Source | Topics |
|--------|--------|--------|
| Taxi | Parquet files | yellow_taxi_stream, green_taxi_stream |
| Weather | Open-Meteo API | weather_stream |

**Output (MinIO):**
- `s3a://bronze/yellow_taxi/` (parquet)
- `s3a://bronze/green_taxi/` (parquet)
- `s3a://bronze/weather/` (parquet)

## Troubleshooting

**Service fail:**
```bash
docker compose logs <service>
```

**Producer lỗi:**
- Kiểm tra: `make status` (Kafka phải Up)
- Kiểm tra: data/raw có 48 parquet files

**Bronze ingestion lỗi:**
- Xem logs: `docker compose logs spark-master`
- Kiểm tra: Kafka topics có data (Kafdrop: http://localhost:9090)

## Chi Tiết Các Lệnh

### `make weather` (~8 sec)
Fetch weather lịch sử 2023-2024 từ Open-Meteo API (NYC Manhattan, hourly, 17,520 records) → Kafka

### `make producer` (~30-60 min)
Stream 48 parquet files (24 yellow + 24 green) với zone enrichment → Kafka

### `make bronze` (realtime)
Spark Streaming consumer: Kafka 3 streams → MinIO Bronze layer (parquet)

---

Xem: [SETUP_GUIDE.md](SETUP_GUIDE.md) | [INFRASTRUCTURE_SETUP.md](INFRASTRUCTURE_SETUP.md)
