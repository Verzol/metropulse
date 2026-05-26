# MetroPulse Progress

## 1. Tổng Quan

MetroPulse là nền tảng Big Data phân tích nhu cầu di chuyển đô thị tại NYC, kết hợp NYC Taxi trip data với dữ liệu thời tiết lịch sử.

Kiến trúc hiện tại:

```text
Kafka -> Bronze -> Silver -> Gold
```

Hạ tầng đang chạy theo mô hình single-host trên GCP VM:

| Thành phần | Trạng thái |
|---|---|
| VM | e2-standard-16, 16 vCPU, 64GB RAM |
| Docker Compose | Đang dùng để chạy toàn bộ services |
| Kafka + Zookeeper | Hoàn thành cho ingestion prototype |
| Spark 3.5.1 | 1 master, 2 workers |
| MinIO | Lưu Bronze/Silver/Gold theo S3-compatible paths |
| Airflow | Đã triển khai webserver, scheduler, metadata Postgres |

Tài liệu chính:

- `README.md`: giới thiệu ngắn về hệ thống và nhóm.
- `docs/SETUP_GUIDE.md`: setup, SSH, cấu hình, chạy pipeline và troubleshooting.
- `docs/MAKEFILE_GUIDE.md`: danh sách lệnh `make`.
- `PROGRESS.md`: trạng thái dự án và kết quả đã xác nhận.

## 2. Thành Phần Đã Hoàn Thành

### Infrastructure

Đã có Docker Compose cho:

- Kafka
- Zookeeper
- Kafdrop
- MinIO
- Spark master
- Spark worker 1
- Spark worker 2
- Airflow Postgres
- Airflow webserver
- Airflow scheduler

Airflow Postgres chỉ dùng làm metadata DB của Airflow, không dùng để lưu dữ liệu taxi/weather.

### Producer Layer

Đã có:

- Taxi producer đọc parquet local và gửi JSON payload vào Kafka.
- Weather producer lấy dữ liệu thời tiết lịch sử từ Open-Meteo và gửi vào Kafka.
- Producer checkpoint local `.producer_checkpoint.json` để tránh replay toàn bộ dữ liệu ngoài ý muốn.

Kafka topics:

```text
nyc_taxi_yellow
nyc_taxi_green
weather_stream
```

### Bronze Layer

Bronze đã hoàn thành logic chính:

- đọc Kafka bằng Spark Structured Streaming;
- ghi raw payload xuống MinIO;
- lưu Kafka metadata như topic, partition, offset, timestamp;
- không clean, không deduplicate, không enrich;
- có checkpoint cho fault tolerance.

Output:

```text
s3a://bronze/yellow_taxi/
s3a://bronze/green_taxi/
s3a://bronze/weather/
s3a://bronze/checkpoints/
```

### Silver Layer

Silver Layer đã hoàn thành Phase 2.

Đã có các job:

| Job | File |
|---|---|
| Silver transform | `src/processing/silver_transform.py` |
| Silver Core transform | `src/processing/silver_core_transform.py` |
| Silver quality check | `src/quality/silver_quality_check.py` |
| Silver Clean transform | `src/processing/silver_clean_transform.py` |

Output hiện có trong MinIO:

```text
s3a://silver/hourly_weather/
s3a://silver/taxi_weather_trips/
s3a://silver/taxi_weather_trips_core/
s3a://silver/taxi_weather_trips_clean/
s3a://silver/quality_reports/silver_core_quality/latest/
```

Quality artifact đang lưu của Core/Clean snapshot:

```text
Silver Core quality report: _SUCCESS
Quality failed checks: 0
Silver Clean rows: 80,922,997
Gold candidate rows: 78,272,751
Non-candidate rows: 2,650,246
```

Lưu ý: artifact này chứng minh chất lượng nội tại của Core/Clean snapshot đã ghi, nhưng không chứng minh cùng lần materialization với `taxi_weather_trips` hiện tại; EDA đã phát hiện row-count mismatch giữa enriched và Core.

Kiểm chứng hai bảng được chọn làm nguồn Gold ngày `2026-05-26`:

```text
hourly_weather rows / distinct weather hours: 17,542 / 17,542
hourly_weather duplicate hours: 0
hourly_weather critical weather nulls: 0
taxi_weather_trips_core rows: 80,922,997
taxi_weather_trips_core duplicate trip-key groups: 0
taxi_weather_trips_core critical nulls: 0
Core rows không tìm thấy weather_hour tương ứng: 0
Core/weather feature mismatches sau khi cast về Core schema: 0
Core outlier rows cần policy ở Gold: 2,650,246
```

Dung lượng Silver trong MinIO tại thời điểm kiểm tra:

```text
hourly_weather              876K
taxi_weather_trips          5.0G
taxi_weather_trips_core     2.5G
taxi_weather_trips_clean    2.6G
quality_reports             40K
```

Cleaning rules hiện tại:

- drop rows thiếu critical columns như pickup time, location, fare, weather;
- tạo missing flags cho `passenger_count`, `ratecode_id`, `payment_type`, `congestion_surcharge`, `airport_fee`;
- tạo clean columns:
  - `passenger_count_clean`: null -> `1`;
  - `ratecode_id_clean`: null -> `99`;
  - `payment_type_clean`: null -> `0`;
  - `congestion_surcharge_clean`: null -> `0.00`;
  - `airport_fee_clean`: null -> `0.00`;
- thêm `is_gold_candidate` để Gold/ML lọc trips hợp lệ, không outlier.

### Gold Layer

Gold Layer đã hoàn thành phần ML-ready datasets, quality checks và dashboard marts.

Đã có các job:

| Job | File |
|---|---|
| Gold transform | `src/processing/gold_transform.py` |
| Gold quality check | `src/quality/gold_quality_check.py` |
| Gold dashboard marts | `src/processing/gold_dashboard_marts.py` |

Output hiện có trong MinIO:

```text
s3a://gold/gold_demand_features/
s3a://gold/gold_fare_tip_features/
s3a://gold/quality_reports/gold_quality/latest/
s3a://gold/dashboard_hourly_demand_kpi/
s3a://gold/dashboard_zone_summary/
s3a://gold/dashboard_payment_tip_summary/
```

Quality artifact đang lưu của Gold snapshot:

```text
Gold quality report: _SUCCESS
Quality checks: 73
Quality failed checks: 0
```

Kiểm chứng các bảng Gold ngày `2026-05-26`:

| Dataset | Rows | Size | Files | Partition directories |
|---|---:|---:|---:|---:|
| `gold_demand_features` | 1,977,231 | 11.19 MB | 25 | 24 |
| `gold_fare_tip_features` | 78,079,876 | 734.90 MB | 25 | 24 |
| `quality_reports/gold_quality/latest` | 73 quality checks | 16.29 KB | 2 | 0 |
| `dashboard_hourly_demand_kpi` | 17,542 | 546.59 KB | 25 | 24 |
| `dashboard_zone_summary` | 263 | 18.10 KB | 2 | 0 |
| `dashboard_payment_tip_summary` | 160 | 107.75 KB | 25 | 24 |

Dung lượng Gold trong MinIO tại thời điểm kiểm tra:

```text
gold_demand_features              11.19 MB
gold_fare_tip_features            734.90 MB
dashboard_hourly_demand_kpi       546.59 KB
dashboard_zone_summary            18.10 KB
dashboard_payment_tip_summary     107.75 KB
quality_reports                   16.29 KB
```

Gold rules hiện tại:

- đọc từ `s3a://silver/taxi_weather_trips_core/`;
- lọc trips hợp lệ bằng `is_valid_distance = true`, `is_valid_fare = true`, `is_outlier_trip = false`;
- tạo `GOLD_DEMAND_FEATURES` với grain `pu_location_id x pickup_hour` cho demand prediction;
- tạo `GOLD_FARE_TIP_FEATURES` với grain một dòng là một taxi trip hợp lệ cho fare/tip estimation;
- loại fare/tip outlier bằng policy `2.5 <= fare_amount <= 300`, `trip_distance > 0`, `tip_amount >= 0`, `tip_percent <= 100`;
- tạo dashboard marts đã aggregate sẵn để Power BI không phải đọc trực tiếp bảng trip-level lớn;
- ghi tất cả outputs ở dạng Parquet dataset trong bucket `gold`, không tạo managed table trong SQL/metastore.

Trạng thái phục vụ Hive:

- Gold đã sẵn sàng để đăng ký Hive external tables.
- Các bảng partition theo `pickup_year_month`: `gold_demand_features`, `gold_fare_tip_features`, `dashboard_hourly_demand_kpi`, `dashboard_payment_tip_summary`.
- Bảng không partition: `dashboard_zone_summary`.
- Sau khi tạo external tables, cần chạy `MSCK REPAIR TABLE` cho các bảng có partition.

### Airflow

Đã có DAG:

```text
dags/metropulse_silver_pipeline_dag.py
dags/metropulse_gold_pipeline_dag.py
```

Flow hiện tại:

```text
start
-> validate_project_root
-> validate_docker_access
-> check_required_services
-> run_silver_transform
-> run_silver_core_transform
-> run_silver_quality_check
-> run_silver_clean_transform
-> finish
```

Airflow chỉ orchestrate shell scripts. Spark vẫn xử lý dữ liệu lớn.

Gold flow:

```text
start
-> validate_project_root
-> validate_docker_access
-> check_required_services
-> run_gold_transform
-> run_gold_quality_check
-> run_gold_dashboard_marts
-> finish
```

Trước khi handoff chính thức, nên trigger DAG này một lần trên Airflow UI để có mốc end-to-end successful run.

## 3. Lệnh Chạy Chính

Khởi động services:

```bash
make start
make airflow-init
make airflow-up
```

Chạy pipeline theo từng bước:

```bash
make weather
make producer
make bronze
make silver
make silver-core
make silver-quality
make silver-clean
make gold
make gold-quality
make gold-dashboard
```

Nếu Silver Core đã có sẵn và chỉ muốn chạy Phase 2:

```bash
make silver-quality
make silver-clean
```

## 4. Handoff Sang Gold Layer

Nguồn dữ liệu chính cho Gold Layer:

```text
s3a://silver/taxi_weather_trips_core/
```

Gold output sẵn sàng handoff:

```text
s3a://gold/gold_demand_features/
s3a://gold/gold_fare_tip_features/
s3a://gold/quality_reports/gold_quality/latest/
s3a://gold/dashboard_hourly_demand_kpi/
s3a://gold/dashboard_zone_summary/
s3a://gold/dashboard_payment_tip_summary/
```

Lệnh chạy chính:

```bash
make gold
make gold-quality
make gold-dashboard
```

Trạng thái hiện tại:

```text
Gold transform: done
Gold quality: pass 73/73
Gold dashboard marts: done
Hive external table registration: ready
```

Ghi chú handoff: Gold là Parquet dataset trên MinIO, nên bước tiếp theo là tạo Hive external tables trỏ tới các path trên. Không cần tạo managed table hoặc copy dữ liệu sang nơi khác.

## 5. Việc Cần Làm Tiếp

1. Trigger Airflow DAG `metropulse_silver_pipeline` một lần để xác nhận orchestration end-to-end.
2. Đồng bộ lại Silver outputs bằng một lần chạy end-to-end trước mốc nộp nếu cần lineage cùng một run.
3. Trigger Airflow DAG `metropulse_gold_pipeline` để xác nhận orchestration end-to-end gồm transform, quality và dashboard marts.
4. Tạo Hive DDL cho 5 Gold external tables và chạy `MSCK REPAIR TABLE` với các bảng partition theo `pickup_year_month`.
5. Kết nối Power BI vào các dashboard marts thay vì đọc trực tiếp bảng trip-level `gold_fare_tip_features`.
6. Chuẩn bị ML notebooks/jobs: XGBoost dùng `GOLD_DEMAND_FEATURES`, LightGBM dùng `GOLD_FARE_TIP_FEATURES`; riêng tip model nên lọc `payment_type = 1`.
7. Cân nhắc tạo full zero-demand grid (`all_zones x all_hours`) nếu forecasting model cần học cả giờ không có chuyến.
8. Cân nhắc tăng VM disk lên 150GB-200GB trước khi chạy nhiều Gold/ML jobs.

## 6. Ghi Chú Git

Nên commit source code, scripts, DAG, docs và `.env.example`.

Không commit:

- `.env`
- `.producer_checkpoint.json`
- `airflow/logs/`
- MinIO/Docker volumes
- parquet/raw data lớn
- `__pycache__/`
