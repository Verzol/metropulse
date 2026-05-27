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
| `make start` | Khởi động tất cả services (Kafka, MinIO, Spark, Airflow, PostgreSQL Warehouse) |
| `make stop` | Dừng services (giữ data) |
| `make restart` | Restart |
| `make status` | Kiểm tra trạng thái |
| `make logs` | Xem logs realtime |
| `make airflow-init` | Khởi tạo Airflow metadata DB và admin user |
| `make airflow-up` | Start Airflow webserver + scheduler |
| `make airflow-logs` | Xem logs Airflow |
| `make airflow-dags` | Liệt kê DAGs trong Airflow |
| `make warehouse-up` | Khởi động PostgreSQL Data Warehouse riêng |
| `make warehouse-init` | Tạo schemas/tables/roles nền của warehouse |
| `make warehouse-status` | Kiểm tra schemas và serving tables của warehouse |
| `make warehouse-ml-access` | Tạo/rotate login read-only và xác minh quyền truy cập cho ML |
| `make warehouse-dashboard-access` | Tạo/rotate login read-only và xác minh quyền truy cập cho dashboard |
| `make pgadmin-up` | Khởi động pgAdmin UI để kiểm tra warehouse |
| `make pgadmin-logs` | Xem logs của pgAdmin |

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
| `make gold-publish-ml` | Publish Gold demand features sang PostgreSQL và validate source-target |
| `make gold-publish-dashboard` | Publish ba Gold dashboard marts sang PostgreSQL và validate source-target |
| `make gold-publish-serving` | Publish/validate đồng thời serving tables cho ML và dashboard |
| `make warehouse-quality` | Validate một PostgreSQL publication đang pending |
| `make dashboard-warehouse-quality` | Validate các dashboard publications đang pending |

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
make warehouse-init
make warehouse-ml-access
make warehouse-dashboard-access
make pgadmin-up
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
make gold-publish-serving
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

Gold DAG hiện chạy tuần tự Gold transform, Gold quality, dashboard marts, PostgreSQL ML publication/validation rồi dashboard publication/validation. Chạy `make warehouse-up` hoặc `make start` trước khi trigger DAG; DAG yêu cầu container warehouse đã sẵn sàng thay vì recreate service từ trong Airflow. Airflow chỉ gọi runner; Spark vẫn thực hiện ETL/JDBC validation.

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
- PostgreSQL Warehouse: `localhost:5433` qua SSH tunnel
- pgAdmin: `http://localhost:5050` qua SSH tunnel

Thành viên SSH vào cùng VM và mở tunnel cho các cổng quản trị/database cần dùng; PostgreSQL và pgAdmin không publish trực tiếp ra public IP. Xem [SETUP_GUIDE.md](SETUP_GUIDE.md).

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

**PostgreSQL Warehouse Serving:**

- MinIO vẫn là Data Lake và source of truth của Gold Parquet.
- PostgreSQL database `metropulse_dw` là serving warehouse độc lập với Airflow metadata DB.
- Schema `ml` chứa bản publish đã validate của `gold_demand_features` cho ML.
- Schema `mart` chứa ba bảng đã validate: `dashboard_hourly_demand_kpi`, `dashboard_zone_summary`, `dashboard_payment_tip_summary`.
- Schema `staging` chỉ phục vụ Spark publication; consumer roles không có quyền `USAGE`.
- Schema `audit` lưu lịch sử publish/validation.
- Login ML thực tế được provision bằng `make warehouse-ml-access` và chỉ kế thừa quyền đọc từ `ml_reader`.
- Login dashboard được provision bằng `make warehouse-dashboard-access` và chỉ kế thừa quyền đọc từ `dashboard_reader`.
- Không full-load `gold_fare_tip_features` trong MVP do bảng có hơn 78 triệu dòng.
- Consumer ML đọc `ml.gold_demand_features` bằng giờ chuẩn `America/New_York`; xem [POSTGRES_WAREHOUSE_ML_HANDOFF.md](POSTGRES_WAREHOUSE_ML_HANDOFF.md).
- Dashboard đọc các bảng `mart.dashboard_*`; xem [POSTGRES_WAREHOUSE_DASHBOARD_HANDOFF.md](POSTGRES_WAREHOUSE_DASHBOARD_HANDOFF.md).
- pgAdmin nạp sẵn connections quản trị, ML read-only và Dashboard read-only; password PostgreSQL không nằm trong server definition.

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

### `make gold-publish-ml`
Full-refresh publication cho ML: khởi tạo PostgreSQL warehouse nếu cần, Spark đọc `s3a://gold/gold_demand_features/`, ghi private staging qua JDBC, promote transactionally vào `ml.gold_demand_features`, rồi đối chiếu source-target và ghi audit. JDBC write mặc định dùng `4` partitions để kiểm soát tải lên PostgreSQL single-host. Validation gồm `7` metrics, bao gồm snapshot timestamp.

### `make gold-publish-dashboard`
Full-refresh publication cho ba dashboard marts: Spark ghi các private staging tables, SQL promote transactionally vào schema `mart`, sau đó validate `6` metrics trên từng bảng. Các mart nhỏ được ghi với một JDBC writer để tránh connection pressure không cần thiết.

### `make gold-publish-serving`
Entry point refresh Serving Layer đầy đủ: lần lượt publish/validate ML feature table và ba dashboard marts.

### `make warehouse-quality`
Chạy lại validation cho publication mới nhất còn ở trạng thái `started`; lệnh này không tạo publication mới. Kết quả ghi vào `audit.validation_results` và cập nhật trạng thái run.

### `make dashboard-warehouse-quality`
Chạy lại validation cho ba dashboard publication runs còn ở trạng thái `started`; không tạo publication mới.

Kiểm tra quality report:

```python
quality = spark.read.json("s3a://gold/quality_reports/gold_quality/latest/")
quality.filter("status = 'fail'").show(truncate=False)
```

### `make airflow-init`
Initializes Airflow Postgres metadata DB and creates the admin user from `.env`.

### `make airflow-up`
Starts Airflow webserver and scheduler. Use this to trigger `metropulse_silver_pipeline`.

### `make warehouse-init`
Starts PostgreSQL Warehouse and chạy SQL foundation idempotent: tạo schemas `ml`, `mart`, `audit`, `staging`, ML/dashboard target tables, audit tables và read-only group roles. Cần cấu hình `WAREHOUSE_POSTGRES_PASSWORD` trong `.env` trước khi chạy; warehouse chỉ bind `127.0.0.1:5433` trên VM.

### `make warehouse-status`
Kiểm tra warehouse schemas, ML/dashboard serving tables và số dòng hiện có. Quality source-target chi tiết được ghi bởi publication runners.

### `make warehouse-ml-access`
Tạo hoặc rotate login `WAREHOUSE_ML_READER_USER` bằng password trong `.env`, grant group role `ml_reader`, rồi xác minh login có thể đọc `ml.gold_demand_features` nhưng không có quyền ghi. Credential thật không được commit vào Git.

### `make warehouse-dashboard-access`
Tạo hoặc rotate login `WAREHOUSE_DASHBOARD_READER_USER`, grant group role `dashboard_reader`, rồi xác minh login chỉ đọc được ba `mart.dashboard_*` tables và không truy cập private staging.

### `make pgadmin-up`
Khởi động pgAdmin web UI trên `127.0.0.1:5050` của VM. Từ máy cá nhân, mở SSH tunnel và truy cập `http://localhost:5050`; đăng nhập bằng cấu hình `PGADMIN_DEFAULT_EMAIL` và `PGADMIN_DEFAULT_PASSWORD` trong `.env`.

---

Xem thêm:

- [SETUP_GUIDE.md](SETUP_GUIDE.md): setup, infra, config và troubleshooting.
- [../PROGRESS.md](../PROGRESS.md): kế hoạch chạy và kết quả đã hoàn thành.
