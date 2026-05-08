# METROPULSE - HƯỚNG DẪN SETUP

## Yêu Cầu Hệ Thống

| Thông tin | Chi tiết |
|-----------|---------|
| OS | Linux, macOS, Windows (WSL2) |
| Docker | 20.10+ (có docker-compose) |
| Python | 3.11+ |
| RAM | 8GB+ |
| Disk | 20GB+ trống |

## Setup Nhanh

### Bước 1: Clone & Setup Python

```bash
git clone <repo>
cd metropulse
python3 -m venv .venv
source .venv/bin/activate
```

### Bước 2: Cài Package

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Bước 3: Cấu Hình Environment

```bash
cat > .env << 'ENVEOF'
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=metropulse2026
GCP_EXTERNAL_IP=localhost
ENVEOF
```

### Bước 4: Khởi Động Docker

```bash
docker compose up -d
```

Xác minh:
```bash
docker compose ps
```

Các service: Zookeeper, Kafka, MinIO, Spark (1 master + 2 workers), Kafdrop

### Bước 5: Tải Dữ Liệu Taxi

```bash
chmod +x download_data.sh
./download_data.sh
```

Kiểm tra:
```bash
ls data/raw/*.parquet | wc -l  # Cần 48 files
```

## Kiểm Tra Kết Nối

### Kafka

```bash
python3 -c "import socket; s = socket.socket(); print('✓ Kafka OK' if s.connect_ex(('localhost', 9092)) == 0 else '✗ Kafka FAIL')"
```

### MinIO

```bash
curl -s -u admin:metropulse2026 http://localhost:9001 > /dev/null && echo "✓ MinIO OK" || echo "✗ MinIO FAIL"
```

## Truy Cập Services

| Service | URL |
|---------|-----|
| Kafdrop | http://localhost:9090 |
| MinIO | http://localhost:9001 |
| Spark Master | http://localhost:8080 |

MinIO login: `admin` / `metropulse2026`

## Packages

| Package | Mục đích |
|---------|---------|
| kafka-python | Producer/Consumer |
| pyspark | Spark Streaming |
| pandas | Data Processing |
| requests | HTTP API |
| python-dotenv | Config |

## Troubleshooting

| Vấn đề | Giải pháp |
|--------|----------|
| Container không start | `docker compose logs <service>` |
| Python package lỗi | `pip install --upgrade pip` |
| Kafka connection fail | Chờ 30s, kiểm tra `.env` |
| Port đã dùng | Dừng service xung đột |

## Next Steps

Xem [MAKEFILE_GUIDE.md](MAKEFILE_GUIDE.md) để chạy pipeline
