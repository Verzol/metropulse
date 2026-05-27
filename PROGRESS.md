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
| PostgreSQL Warehouse | ML/dashboard serving tables đã publish, validate và cấp read-only access |
| pgAdmin | UI kiểm tra PostgreSQL Warehouse qua SSH tunnel |

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
- PostgreSQL Warehouse (`warehouse-postgres`)

Airflow Postgres chỉ dùng làm metadata DB của Airflow, không dùng để lưu dữ liệu taxi/weather.
PostgreSQL Warehouse là database riêng để nhận các Gold dataset được publish cho consumer; MinIO vẫn là source of truth.

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
Quality checks: 75
Quality failed checks: 0
```

Hai feature datasets được kiểm chứng và dashboard marts được build lại ngày `2026-05-27`:

| Dataset | Rows | Size | Files | Partition directories |
|---|---:|---:|---:|---:|
| `gold_demand_features` | 1,977,231 | 11.19 MB | 25 | 24 |
| `gold_fare_tip_features` | 78,079,876 | 734.90 MB | 25 | 24 |
| `quality_reports/gold_quality/latest` | 75 quality checks | latest report | 2 | 0 |
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
quality_reports                   refreshed (75 checks; size not remeasured)
```

Baseline phục vụ publication `gold_demand_features` sang PostgreSQL, thu từ quality run ngày `2026-05-27`:

| Metric | Value |
|---|---:|
| Row count | 1,977,231 |
| Total demand | 78,272,751 |
| `pickup_year_month` count | 24 |
| Min `pickup_hour` | `2023-01-01 05:00:00` |
| Max `pickup_hour` | `2025-01-01 04:00:00` |
| Quality result | pass `75/75` |

Quality job hiện ghi thêm `total_demand` và `pickup_year_month_count` vào report để bước publish sang PostgreSQL có thể đối chiếu source-target mà không chỉ dựa vào row count.

Gold rules hiện tại:

- đọc từ `s3a://silver/taxi_weather_trips_core/`;
- lọc trips hợp lệ bằng `is_valid_distance = true`, `is_valid_fare = true`, `is_outlier_trip = false`;
- tạo `GOLD_DEMAND_FEATURES` với grain `pu_location_id x pickup_hour` cho demand prediction;
- tạo `GOLD_FARE_TIP_FEATURES` với grain một dòng là một taxi trip hợp lệ cho fare/tip estimation;
- loại fare/tip outlier bằng policy `2.5 <= fare_amount <= 300`, `trip_distance > 0`, `tip_amount >= 0`, `tip_percent <= 100`;
- tạo dashboard marts đã aggregate sẵn để Power BI không phải đọc trực tiếp bảng trip-level lớn;
- ghi tất cả outputs ở dạng Parquet dataset trong bucket `gold`, không tạo managed table trong SQL/metastore.

Quyết định Data Warehouse / Serving Layer:

- MinIO tiếp tục là Data Lake và source of truth cho Bronze, Silver và Gold Parquet.
- PostgreSQL sẽ là Data Warehouse / serving layer cho ML và dashboard consumers; Hive không nằm trong MVP mới.
- MVP PostgreSQL publish trước `gold_demand_features` vì bảng này phù hợp nhu cầu demand forecasting và có quy mô vừa.
- Chưa full-load `gold_fare_tip_features` vào PostgreSQL do bảng có `78,079,876` dòng; chỉ thực hiện sau khi có nhu cầu ML rõ ràng và benchmark tài nguyên.
- Ba dashboard marts đã được publish sang PostgreSQL schema `mart`, validate và cấp quyền dashboard read-only.

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
-> initialize_warehouse
-> publish_gold_demand_to_postgres
-> validate_postgres_warehouse_publication
-> publish_dashboard_marts_to_postgres
-> validate_postgres_dashboard_publication
-> finish
```

Phase 5 mở rộng Gold DAG để điều phối publication/validation PostgreSQL. Airflow vẫn chỉ gọi shell runners; dữ liệu Gold và source-target validation tiếp tục được Spark xử lý ngoài Airflow process.

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
Gold quality: pass 75/75
Gold dashboard marts: done
PostgreSQL ML publication: done; validation pass 7/7, gồm snapshot lineage
PostgreSQL dashboard publication: done; validation pass 6/6 cho từng mart
```

Ghi chú handoff: Gold vẫn là Parquet dataset trên MinIO. PostgreSQL chứa bản publish có kiểm soát của `gold_demand_features` và ba `dashboard_*` marts; PostgreSQL không thay thế MinIO source of truth và không dùng chung database metadata của Airflow.

### PostgreSQL Warehouse Foundation

Phase 2 triển khai serving foundation:

```text
Service: warehouse-postgres
Database: metropulse_dw
Host port: 5433
Schemas: ml, mart, audit, staging
```

Phương án đã chốt: warehouse bind `127.0.0.1:5433` trên VM; thành viên truy cập cùng dữ liệu thông qua SSH tunnel. Public IP của VM dùng cho SSH, không expose PostgreSQL trực tiếp ra internet.

Objects chuẩn bị cho MVP:

| Object | Mục đích |
|---|---|
| `ml.gold_demand_features` | Table đích cho demand forecasting features sau khi Spark JDBC publish |
| `audit.publish_run_history` | Lưu trạng thái mỗi lần publish |
| `audit.validation_results` | Lưu kết quả đối chiếu MinIO source với PostgreSQL target |
| `ml_reader` | Read-only group role được login consumer ML kế thừa |
| `dashboard_reader` | Read-only group role cho ba dashboard marts |
| `staging` | Private publication schema; không cấp `USAGE` cho consumer |

SQL initialization nằm tại `sql/postgres/init_warehouse.sql` và chạy bằng:

```bash
make warehouse-init
make warehouse-status
```

Phase 2 chỉ dựng database contract; Phase 3 bên dưới đã nạp dữ liệu vào bảng này.

Xác minh foundation ngày `2026-05-27`:

| Check | Kết quả |
|---|---|
| Container `warehouse-postgres` | healthy |
| Bind address | `127.0.0.1:5433 -> 5432` |
| Database timezone | `America/New_York` |
| Schemas | `ml`, `mart`, `audit`, `staging` tồn tại |
| Serving table | `ml.gold_demand_features` tồn tại |
| Audit tables | `audit.publish_run_history`, `audit.validation_results` tồn tại |
| Serving row count trước publication | `0` (đúng kỳ vọng Phase 2) |

### PostgreSQL ML Publication

Phase 3 triển khai đường publish cho dataset demand forecasting:

```text
s3a://gold/gold_demand_features/
-> Spark JDBC staging: staging.gold_demand_features_staging
-> transaction promote: ml.gold_demand_features
-> Spark source-target validation
-> audit.publish_run_history + audit.validation_results
```

Các thành phần triển khai:

| Component | Vai trò |
|---|---|
| `src/serving/publish_gold_to_postgres.py` | Đọc Gold Parquet, ghi staging bằng Spark JDBC và kiểm tra metrics trước promotion |
| `sql/postgres/promote_gold_demand_features.sql` | Promote staging vào serving table trong transaction và tạo audit run |
| `src/quality/postgres_warehouse_quality_check.py` | Đối chiếu MinIO Gold với PostgreSQL target và ghi validation audit |
| `scripts/run_gold_postgres_publish_docker.sh` | Chạy publisher rồi promotion trong Docker |
| `scripts/run_postgres_warehouse_quality_docker.sh` | Chạy validation và đóng trạng thái audit run |

Lệnh chạy:

```bash
make gold-quality
make gold-publish-ml
make warehouse-status
```

`make gold-publish-ml` giới hạn JDBC write ở `4` partitions mặc định để tránh tạo quá nhiều concurrent connections/write pressure lên PostgreSQL single-host. Đây là full refresh MVP của bảng demand có quy mô vừa; chưa áp dụng cho bảng fare/tip lớn.

Kết quả baseline publication ML ngày `2026-05-27` (các run mới hơn tiếp tục giữ cùng business totals):

| Metric | Giá trị |
|---|---:|
| Audit publish run baseline | `1` (`passed`) |
| Rows in `ml.gold_demand_features` | `1,977,231` |
| Total demand | `78,272,751` |
| Distinct `pickup_year_month` | `24` |
| SQL min `pickup_hour` (`America/New_York`) | `2023-01-01 00:00:00` |
| SQL max `pickup_hour` (`America/New_York`) | `2024-12-31 23:00:00` |
| Source-target validation | `7/7 pass`, gồm `gold_processed_timestamp` lineage |
| PostgreSQL total relation size | `271 MB` |

Trong audit của Spark JDBC, timestamp source-target được đối chiếu theo representation của Spark (`2023-01-01 05:00:00` tới `2025-01-01 04:00:00`). Khi consumer query PostgreSQL, sử dụng giờ địa phương đã chuẩn hóa `America/New_York` như bảng trên; không chuyển timezone thêm lần nữa.

Tài liệu sử dụng cho nhóm ML: `docs/POSTGRES_WAREHOUSE_ML_HANDOFF.md`.

### PostgreSQL ML Read-Only Access

Phase 4 triển khai handoff credential cho ML consumers; dashboard serving được hoàn thiện ở mục tiếp theo:

```text
ml_reader (NOLOGIN group role)
-> metropulse_ml_reader (LOGIN, credential trong .env)
-> SELECT-only access to ml.gold_demand_features
```

Các thành phần:

| Component | Vai trò |
|---|---|
| `sql/postgres/create_ml_reader_login.sql` | Tạo/rotate login, set timezone và grant `ml_reader` |
| `sql/postgres/verify_ml_reader_access.sql` | Xác minh ACL read-only không tạo thay đổi dữ liệu |
| `scripts/setup_ml_reader_access_docker.sh` | Chạy provisioning, ACL validation và login read test |
| `make warehouse-ml-access` | Entry point vận hành cho quản trị viên pipeline |

Credential thật lưu trong `.env` bị Git ignore và phải phân phối ngoài repository. ML workload không sử dụng owner account `metropulse_dw`.

Operational security note: `.env` chứa owner credential dùng cho publisher. Read-only isolation chỉ hoàn chỉnh khi ML consumer không có quyền đọc `.env` hoặc quản trị Docker; nếu nhóm chia sẻ cùng Unix account/workspace/Docker access, cần xem đây là trust-based prototype hoặc dùng secret management và tách quyền trước handoff chính thức.

Xác minh Phase 4 ngày `2026-05-27`:

| Check | Kết quả |
|---|---|
| Consumer login | `metropulse_ml_reader`, `LOGIN=true` |
| Inherits group role | `ml_reader=true` |
| Timezone khi login | `America/New_York` |
| Can query `ml.gold_demand_features` | `true`, đọc được `1,977,231` rows |
| Can create in schema `ml` | `false` |
| Can `INSERT` / `UPDATE` / `DELETE` features | `false` / `false` / `false` |
| Can use private schema `staging` | `false` |

### PostgreSQL Dashboard Publication Và Access

Serving layer cho dashboard đã được triển khai ngày `2026-05-27`:

| Table | Rows | Measure đã đối chiếu |
|---|---:|---:|
| `mart.dashboard_hourly_demand_kpi` | `17,542` | total demand `78,272,751` |
| `mart.dashboard_zone_summary` | `263` | total demand `78,272,751` |
| `mart.dashboard_payment_tip_summary` | `160` | trip count `78,079,876` |

Publisher ghi vào private `staging.dashboard_*_staging`, promote ba bảng trong một transaction, sau đó validator đối chiếu `6/6` metrics cho mỗi table, gồm timestamp lineage từ Gold và thời điểm build dashboard. Login `metropulse_dashboard_reader` chỉ có `SELECT` trên ba marts; không có quyền ghi, tạo object hoặc dùng schema `staging`.

### pgAdmin Warehouse Inspection

Đã bổ sung web UI phục vụ kiểm tra PostgreSQL:

```text
Service: pgadmin
Host binding: 127.0.0.1:5050
Persistent config volume: pgadmin_data
Preloaded servers:
- MetroPulse Warehouse -> warehouse-postgres:5432/metropulse_dw (admin)
- MetroPulse ML Read Only -> warehouse-postgres:5432/metropulse_dw (ML consumer)
- MetroPulse Dashboard Read Only -> warehouse-postgres:5432/metropulse_dw (dashboard consumer)
```

Phương án đã chốt: pgAdmin không expose public port trên VM; người dùng từ máy cá nhân truy cập qua SSH tunnel tới `http://localhost:5050`. Password đăng nhập pgAdmin và password database nằm trong `.env`, không commit vào Git. Server definition được commit không lưu password database.

Xác minh triển khai ngày `2026-05-27`:

| Check | Kết quả |
|---|---|
| Container `pgadmin` | `Up`, image `dpage/pgadmin4:9.15` |
| Host binding | `127.0.0.1:5050 -> 80` |
| HTTP check | redirect tới `/login` |
| Preloaded connections | `MetroPulse Warehouse`, `MetroPulse ML Read Only`, `MetroPulse Dashboard Read Only` |
| Warehouse sau khi khởi động UI | `1,977,231` rows; audit publications được bảo toàn |

Quyết định truy cập cuối ngày `2026-05-27`: cả nhóm làm việc trên cùng VM; `warehouse-postgres` và `pgadmin` giữ localhost binding, mỗi thành viên mở SSH tunnel khi dùng UI/database client từ máy cá nhân. Cách này giữ chung dataset mà không truyền credential PostgreSQL/pgAdmin trên HTTP công khai.

Xác minh theo phương án đã chốt:

| Check | Kết quả |
|---|---|
| `warehouse-postgres` binding | `127.0.0.1:5433 -> 5432`, healthy |
| `pgadmin` binding | `127.0.0.1:5050 -> 80`, Up |
| pgAdmin HTTP check trên VM | redirect tới `/login` |
| Serving data preserved | `1,977,231` rows; total demand `78,272,751` |
| Audit preserved | các publish runs trước đó vẫn `passed` |

### Airflow PostgreSQL Publication Orchestration

Phase 5 tích hợp serving publication vào `metropulse_gold_pipeline`:

| Task | Vai trò |
|---|---|
| `initialize_warehouse` | Initialize schemas/tables idempotently trên container warehouse đang chạy |
| `publish_gold_demand_to_postgres` | Chạy Spark JDBC staging và transactional promotion |
| `validate_postgres_warehouse_publication` | Chạy Spark source-target checks và cập nhật audit status |
| `publish_dashboard_marts_to_postgres` | Ghi private staging và promote ba dashboard marts |
| `validate_postgres_dashboard_publication` | Đối chiếu ba marts với MinIO source và cập nhật audit status |

DAG validate thêm publisher, validation runner và SQL promotion artifacts trước khi chạy. Task prerequisite yêu cầu `warehouse-postgres` đã chạy (khởi động ngoài DAG bằng `make warehouse-up`/`make start`) để Airflow không recreate service có volume bind mount khi gọi Docker từ container. Thứ tự tuần tự sau Gold quality/dashboard giúp PostgreSQL chỉ nhận snapshot Gold đã hoàn thành đầy đủ; đổi lại full DAG refresh sẽ mất thêm thời gian publication trên single-host VM.

MinIO console cũng đã được khôi phục sau khi port `9000/9001` được giải phóng:

| Check | Kết quả |
|---|---|
| MinIO port publish | `0.0.0.0:9000-9001 -> 9000-9001` |
| Console health trên VM | `http://127.0.0.1:9001` trả `HTTP 200` |
| MinIO storage state | `1 Online, 0 Offline` |

Xác minh Phase 5 ngày `2026-05-27`:

| Check | Kết quả |
|---|---|
| Airflow DAG parsing | `metropulse_gold_pipeline` load thành công, không có import errors |
| `initialize_warehouse` qua Airflow | `SUCCESS`, SQL initialization idempotent |
| `publish_gold_demand_to_postgres` qua Airflow | `SUCCESS`, Spark JDBC publish và transactional promote hoàn tất |
| `validate_postgres_warehouse_publication` qua Airflow | `SUCCESS`, source-target validation pass `7/7` |
| Full DAG run | `manual__2026-05-27T09:51:21+00:00`, `success` lúc `2026-05-27T10:00:22+00:00` |
| Latest ML audit publication | run `7`, trạng thái `passed`, validation `7/7` |
| Latest dashboard audits | runs `8`-`10`, trạng thái `passed`, validation `6/6` mỗi table |
| Serving data after Airflow publication | `1,977,231` rows; total demand `78,272,751` |
| Dashboard serving after Airflow publication | hourly `17,542`; zone `263`; payment/tip `160` rows |
| ML read-only ACL preserved | `SELECT=true`, `INSERT=false` |
| Temporary Spark secret cleanup | `/tmp/.env` không còn sau task execution |

Run thủ công ngày `2026-05-27` đã hoàn thành toàn bộ đường `Gold transform -> Gold quality -> dashboard marts -> ML publication/validation -> dashboard publication/validation`, xác nhận Serving Layer có thể handoff cho hai nhóm consumer trên cùng snapshot.

## 5. Việc Cần Làm Tiếp

1. Trigger Airflow DAG `metropulse_silver_pipeline` một lần để xác nhận orchestration end-to-end.
2. Đồng bộ lại Silver outputs bằng một lần chạy end-to-end trước mốc nộp nếu cần lineage cùng một run.
3. Phân phối credential read-only riêng cho thành viên ML và dashboard bằng kênh ngoài Git.
4. Tách Unix user/secret manager nếu cần cô lập secret mạnh hơn mô hình shared VM prototype.
5. Chỉ cân nhắc publish `gold_fare_tip_features` khi nhóm ML xác nhận cần, kèm benchmark partitioned/incremental load.
6. Cân nhắc tạo full zero-demand grid (`all_zones x all_hours`) nếu forecasting model cần học cả giờ không có chuyến.
7. Cân nhắc tăng VM disk lên 150GB-200GB trước khi chạy nhiều Gold/ML jobs.

## 6. Ghi Chú Git

Nên commit source code, scripts, DAG, docs và `.env.example`.

Không commit:

- `.env`
- `.producer_checkpoint.json`
- `airflow/logs/`
- MinIO/Docker volumes
- parquet/raw data lớn
- `__pycache__/`
