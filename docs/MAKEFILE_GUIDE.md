# MetroPulse Makefile Guide

## Các Lệnh

### Setup

| Lệnh | Mục đích |
|------|---------|
| `make venv` | Tạo Python virtual environment |
| `make install` | Cài packages (pip + requirements.txt) |

### Docker Services

| Lệnh | Mục đích |
|------|---------|
| `make start` | Khởi động tất cả services (Kafka, MinIO, Spark, Airflow) |
| `make stop` | Dừng services (giữ data) |
| `make restart` | Restart |
| `make status` | Kiểm tra trạng thái |
| `make logs` | Xem logs realtime |
| `make airflow-init` | Khởi tạo Airflow metadata DB và admin user |
| `make airflow-up` | Start Airflow webserver + scheduler |
| `make airflow-logs` | Xem logs Airflow |
| `make airflow-dags` | Liệt kê DAGs trong Airflow |

### Data Pipeline

| Lệnh | Mục đích |
|------|---------|
| `make weather` | Stream weather data (Open-Meteo → Kafka) |
| `make producer` | Stream taxi data (Parquet → Kafka) |
| `make bronze` | Ingest Kafka → MinIO (Spark Streaming) |
| `make silver` | Build Silver taxi-weather parquet từ Bronze |
| `make silver-core` | Build compact Silver Core từ Silver |
| `make silver-quality` | Chạy quality checks trên Silver Core |
| `make silver-clean` | Build cleaned Silver dataset từ Silver Core |
| `make gold` | Build Gold ML-ready datasets từ Silver Core |
| `make gold-quality` | Chạy quality checks trên Gold datasets |
| `make gold-dashboard` | Build aggregate Gold marts cho Power BI/dashboard |

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
make airflow-init
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

**Silver batch qua CLI:**
```bash
make silver
make silver-core
make silver-quality
make silver-clean
```

**Gold batch qua CLI:**
```bash
make silver
make silver-core
make silver-quality
make gold
make gold-quality
make gold-dashboard
```

`make silver`, `make silver-core`, `make silver-quality` nên chạy lại trước Gold chính thức để đảm bảo lineage sạch giữa Silver enriched và Silver Core.

Nếu đã chạy `make silver-core` trước đó và dữ liệu trong MinIO còn nguyên, có thể chạy thẳng:

```bash
make silver-quality
make silver-clean
```

**Silver batch qua Airflow:**
```bash
make airflow-up
```

Sau đó mở Airflow và trigger DAG:

```text
metropulse_silver_pipeline
```

**Gold batch qua Airflow:**
```bash
make airflow-up
```

Sau đó mở Airflow và trigger DAG:

```text
metropulse_gold_pipeline
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
- Airflow: http://localhost:8088

Nếu đang làm từ máy cá nhân qua GCP VM, nên mở các URL này bằng SSH tunnel. Xem [SETUP_GUIDE.md](SETUP_GUIDE.md).

## Data Streams

| Stream | Source | Topics |
|--------|--------|--------|
| Taxi | Parquet files | nyc_taxi_yellow, nyc_taxi_green |
| Weather | Open-Meteo API | weather_stream |

**Output (MinIO):**
- `s3a://bronze/yellow_taxi/` (parquet)
- `s3a://bronze/green_taxi/` (parquet)
- `s3a://bronze/weather/` (parquet)
- `s3a://silver/hourly_weather/` (parquet)
- `s3a://silver/taxi_weather_trips/` (parquet)
- `s3a://silver/taxi_weather_trips_core/` (parquet)
- `s3a://silver/taxi_weather_trips_clean/` (parquet)
- `s3a://silver/quality_reports/silver_core_quality/latest/` (json)
- `s3a://gold/gold_demand_features/` (parquet)
- `s3a://gold/gold_fare_tip_features/` (parquet)
- `s3a://gold/quality_reports/gold_quality/latest/` (json)
- `s3a://gold/dashboard_hourly_demand_kpi/` (parquet)
- `s3a://gold/dashboard_zone_summary/` (parquet)
- `s3a://gold/dashboard_payment_tip_summary/` (parquet)

Gold layer hiện đọc từ `s3a://silver/taxi_weather_trips_core/`. Core đã chứa weather features đã join theo `pickup_hour` ở timezone `America/New_York`, nên Gold không join lại weather dimension nếu chỉ cần `temperature_f` và `precipitation_mm`.

Logical schemas:

- `GOLD_DEMAND_FEATURES`: 1 row = 1 `pu_location_id` x 1 `pickup_hour`, dùng cho demand prediction.
- `GOLD_FARE_TIP_FEATURES`: 1 row = 1 taxi trip hợp lệ sau khi loại tip outlier, dùng cho fare/tip estimation extension.

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

**Airflow login lỗi:**
- Kiểm tra `AIRFLOW_ADMIN_USERNAME` và `AIRFLOW_ADMIN_PASSWORD` trong `.env`.
- Nếu đổi password sau khi init, chạy lại `make airflow-init` hoặc reset user trong Airflow container.

## Chi Tiết Các Lệnh

### `make weather` (~8 sec)
Fetch weather lịch sử 2023-2024 từ Open-Meteo API (NYC Manhattan, hourly, 17,520 records) → Kafka

### `make producer` (~30-60 min)
Stream 48 parquet files (24 yellow + 24 green) với zone enrichment → Kafka

### `make bronze` (realtime)
Spark Streaming consumer: Kafka 3 streams → MinIO Bronze layer (parquet)

### `make silver`
Spark batch job: Bronze taxi/weather parquet → standardized and enriched Silver parquet.

### `make silver-core`
Spark batch job: Silver enriched parquet → compact Silver Core schema for downstream Gold/ML.

### `make silver-quality`
Spark read-only job: kiểm schema, row count, critical nulls, outlier profile và ghi quality report xuống MinIO.

### `make silver-clean`
Spark batch job: đọc Silver Core, xử lý null nghiệp vụ thành các cột clean/flags và ghi `taxi_weather_trips_clean`.

### `make gold`
Spark batch job: đọc `s3a://silver/taxi_weather_trips_core/`, lọc valid non-outlier trips, tạo `GOLD_DEMAND_FEATURES` và `GOLD_FARE_TIP_FEATURES`, rồi ghi parquet xuống bucket `gold`.

### `make gold-quality`
Spark read-only job: kiểm schema, critical nulls, duplicate demand key, range constraints và ghi report xuống `s3a://gold/quality_reports/gold_quality/latest/`.

### `make gold-dashboard`
Spark batch job: đọc Gold ML-ready datasets, aggregate thành dashboard marts nhẹ cho Power BI gồm hourly demand KPI, pickup zone summary và payment/tip summary.

Kiểm tra quality report:

```python
quality = spark.read.json("s3a://gold/quality_reports/gold_quality/latest/")
quality.filter("status = 'fail'").show(truncate=False)
```

### `make airflow-init`
Initializes Airflow Postgres metadata DB and creates the admin user from `.env`.

### `make airflow-up`
Starts Airflow webserver and scheduler. Use this to trigger `metropulse_silver_pipeline`.

---

Xem thêm:

- [SETUP_GUIDE.md](SETUP_GUIDE.md): setup, infra, config và troubleshooting.
- [../PROGRESS.md](../PROGRESS.md): kế hoạch chạy và kết quả đã hoàn thành.
