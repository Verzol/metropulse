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

### Airflow

Đã có DAG:

```text
dags/metropulse_silver_pipeline_dag.py
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

Trước khi chuyển hẳn sang Gold, nên trigger DAG này một lần trên Airflow UI để có mốc end-to-end successful run.

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
```

Nếu Silver Core đã có sẵn và chỉ muốn chạy Phase 2:

```bash
make silver-quality
make silver-clean
```

## 4. Handoff Sang Gold Layer

Nguồn dữ liệu được chọn cho Gold Layer:

```text
s3a://silver/hourly_weather/
s3a://silver/taxi_weather_trips_core/
```

`hourly_weather` là dimension theo `weather_hour`, phù hợp cho time-series/weather aggregates và có đúng một row mỗi giờ. `taxi_weather_trips_core` là trip-level fact compact đã có các weather features và outlier flags, phù hợp để tạo demand features phân tán bằng Spark.

Vì Core đã mang sẵn weather features khớp với `hourly_weather`, Gold không cần join lại dimension này vào mọi trip chỉ để lấy lại cùng các cột. `hourly_weather` nên được dùng như canonical hourly dimension khi tạo timeline đủ giờ, bảng weather-only hoặc feature aggregates theo giờ.

Core không chứa các cột imputation/missing flags của Silver Clean. Do đó Gold transform phải khai báo rõ policy:

- Với bảng KPI phản ánh toàn bộ trips hợp lệ theo critical schema, có thể giữ toàn bộ Core rows và phân tích riêng `is_outlier_trip`.
- Với bảng ML hoặc KPI cần loại records bất thường, lọc `is_outlier_trip = false` cùng các validity flags; snapshot hiện tại còn `78,272,751` rows sau policy này.
- Với feature dùng `passenger_count`, `ratecode_id`, `payment_type`, `congestion_surcharge` hoặc `airport_fee`, Gold phải xử lý null hoặc tạo missing indicators tường minh.

Hai nguồn đã khớp weather features sau khi chuẩn hóa kiểu dữ liệu (`hourly_weather` lưu numeric dạng `double`, Core lưu dạng `float`/`smallint`). Tuy nhiên Core được materialize ngày `2026-05-13` và weather ngày `2026-05-20`; trước mốc nộp hoặc chạy Gold chính thức nên chạy lại chuỗi Silver liên quan để có lineage cùng một run.

Gold output dự kiến:

```text
s3a://gold/hourly_demand_features/
s3a://gold/zone_weather_correlation/
s3a://gold/pickup_forecast_dataset/
s3a://gold/borough_traffic_aggregation/
```

Gold nên dùng Spark, không dùng Pandas cho dữ liệu lớn.

## 5. Việc Cần Làm Tiếp

1. Trigger Airflow DAG `metropulse_silver_pipeline` một lần để xác nhận orchestration end-to-end.
2. Đồng bộ lại Silver outputs bằng một lần chạy end-to-end trước khi materialize Gold chính thức.
3. Tạo Gold transform đọc từ `hourly_weather` và `taxi_weather_trips_core`, kèm policy outlier/null rõ ràng.
4. Bổ sung Gold quality checks sau khi có feature tables.
5. Cân nhắc tăng VM disk lên 150GB-200GB trước khi chạy nhiều Gold/ML jobs.
6. Sau khi Gold tables ổn định, cân nhắc thêm Hive Metastore hoặc Trino nếu team cần query theo table catalog.

## 6. Ghi Chú Git

Nên commit source code, scripts, DAG, docs và `.env.example`.

Không commit:

- `.env`
- `.producer_checkpoint.json`
- `airflow/logs/`
- MinIO/Docker volumes
- parquet/raw data lớn
- `__pycache__/`
