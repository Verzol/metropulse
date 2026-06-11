# MetroPulse

MetroPulse là nền tảng Big Data phân tích và dự báo nhu cầu di chuyển đô thị tại New York City, sử dụng dữ liệu NYC Taxi kết hợp với dữ liệu thời tiết lịch sử. Dự án triển khai pipeline theo kiến trúc Medallion Lakehouse và cung cấp lớp serving cho ML và dashboard qua PostgreSQL, FastAPI và Streamlit.

## 1. Mục Tiêu

- Ingest dữ liệu taxi và weather dưới dạng event.
- Lưu dữ liệu raw vào Bronze trên MinIO.
- Chuẩn hóa, enrich và quality check dữ liệu ở Silver bằng Spark.
- Tạo Gold datasets cho bài toán Demand, Fare, Tip và dashboard marts.
- Publish dữ liệu serving sang PostgreSQL cho API và dashboard.

## 2. Kiến Trúc Tổng Quan

```text
Taxi + Weather Producers
        ↓
      Kafka
        ↓
 Bronze / MinIO
        ↓
 Silver / MinIO
        ↓
  Gold / MinIO
        ↓
 PostgreSQL Warehouse
        ↓
 FastAPI + Streamlit + ML
```

Các thành phần chính:

- `Kafka`: lớp vận chuyển event.
- `Spark 3.5.1`: xử lý batch/streaming và ETL phân tán.
- `MinIO`: S3-compatible object storage cho Bronze, Silver, Gold và checkpoint.
- `PostgreSQL`: serving warehouse cho ML và dashboard.
- `Airflow 2.9.3`: orchestration layer cho pipeline jobs.
- `FastAPI`: API phục vụ dashboard và inference.
- `Streamlit`: giao diện trực quan hóa và simulator.

## 3. Trạng Thái Hiện Tại

- Bronze: đã có pipeline Kafka → MinIO.
- Silver: đã có enrichment, core schema, clean dataset và quality check.
- Gold: đã có `gold_demand_features`, `gold_fare_tip_features` và dashboard marts.
- PostgreSQL Warehouse: đã có các bảng serving cho ML và dashboard.
- Serving Layer: đã có FastAPI và Streamlit dùng PostgreSQL làm nguồn truy vấn.

Lưu ý:

- Dashboard trong repo hiện dùng `FastAPI + Streamlit`, không dùng Power BI.
- Các mô hình ML chính trong repo là `Demand`, `Fare` và `Tip`.

## 4. Cấu Trúc Repo

```text
src/
  ingestion/      Producer taxi và weather
  processing/     Bronze, Silver, Gold transforms
  quality/        Data quality checks
  serving/        Publish Gold sang PostgreSQL
  dashboard_api/  FastAPI service
  dashboard_app/  Streamlit app

scripts/          Shell scripts chạy Spark/publish jobs trong Docker
ml/               Code train/inference và demo ML
docs/             Setup guide, handoff docs, troubleshooting
reports/          Final report và figures
```

## 5. Yêu Cầu Môi Trường

Khuyến nghị theo môi trường nhóm đang dùng:

- GCP VM
- 16 vCPU / 64 GB RAM
- Docker + Docker Compose
- Python 3
- Git

Ngoài ra cần file `.env` hợp lệ.

Nếu bạn làm trên VM dùng chung của nhóm thì `.env` thường đã có sẵn. Nếu chạy ở môi trường mới, hãy bắt đầu từ `.env.example` và điền lại toàn bộ biến cần thiết.

## 6. Chạy Nhanh Từng Bước

### 6.1 Chuẩn bị môi trường Python

```bash
make venv
make install
```

Kích hoạt môi trường:

```bash
source .venv/bin/activate
```

### 6.2 Khởi động service nền

Khởi động toàn bộ stack Docker:

```bash
make start
```

Khởi tạo Airflow metadata và admin user:

```bash
make airflow-init
make airflow-up
```

Khởi động PostgreSQL warehouse:

```bash
make warehouse-up
make warehouse-init
```

Kiểm tra trạng thái:

```bash
make status
docker compose ps
make warehouse-status
```

### 6.3 Chạy pipeline dữ liệu

#### Bước 1: stream weather và taxi vào Kafka

Mỗi lệnh này thường chạy như một producer riêng:

```bash
make weather
make producer
```

#### Bước 2: ghi Bronze

```bash
make bronze
```

#### Bước 3: build Silver

```bash
make silver
make silver-core
make silver-quality
make silver-clean
```

#### Bước 4: build Gold

```bash
make gold
make gold-quality
make gold-dashboard
```

#### Bước 5: Publish serving sang PostgreSQL

```bash
make gold-publish-ml
make gold-publish-fare-tip
make gold-publish-dashboard
```

Hoặc publish toàn bộ:

```bash
make gold-publish-serving
```

### 6.4 Chạy lớp serving

Mở FastAPI:

```bash
make dashboard-api
```

Mở Streamlit:

```bash
make dashboard-ui
```

### 6.5 Mở pgAdmin

```bash
make pgadmin-up
```

## 7. Luồng Chạy Đề Xuất Cho Người Mới

Nếu bạn chỉ muốn chạy lại hệ thống theo đúng flow chuẩn của repo:

```bash
make venv
make install
make start
make airflow-init
make airflow-up
make warehouse-init
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
make gold-publish-serving
make dashboard-api
make dashboard-ui
```

Lưu ý:

- `make weather` và `make producer` có thể chạy lâu vì chúng là tiến trình nạp dữ liệu.
- Các job Spark và publish nên chạy tuần tự để dễ theo dõi log.
- Nếu bạn đang dùng VM chung của nhóm, nên báo trước khi chạy full pipeline vì khá tốn tài nguyên.

## 8. URL Thường Dùng

Trong môi trường local hoặc qua SSH tunnel:

- MinIO: `http://localhost:9001`
- Spark UI: `http://localhost:8080`
- Airflow: `http://localhost:8088`
- FastAPI: `http://127.0.0.1:8000`
- Streamlit: `http://localhost:8501`
- PostgreSQL Warehouse: `localhost:5433`
- pgAdmin: `http://localhost:5050`

## 9. Kiểm Tra Nhanh Sau Khi Chạy

Kiểm tra service:

```bash
docker compose ps
make status
```

Kiểm tra Airflow DAG:

```bash
make airflow-dags
```

Xem log:

```bash
docker compose logs airflow-webserver --tail=50
docker compose logs spark-master --tail=50
docker compose logs warehouse-postgres --tail=50
```

Kiểm tra warehouse:

```bash
make warehouse-status
```

## 10. Một Số Lệnh Hữu Ích

Dừng services nhưng giữ dữ liệu:

```bash
make stop
```

Khởi động lại:

```bash
make restart
```

Xem log toàn bộ:

```bash
make logs
```

Chạy lại quality cho PostgreSQL serving:

```bash
make warehouse-quality
make fare-tip-warehouse-quality
make dashboard-warehouse-quality
```

## 11. Các Tài Liệu Chính

- [Setup Guide](docs/SETUP_GUIDE.md): hướng dẫn SSH vào VM, làm việc nhóm, cấu hình và troubleshooting.
- [Makefile Guide](docs/MAKEFILE_GUIDE.md): danh sách lệnh `make`.
- [PostgreSQL ML Handoff](docs/POSTGRES_WAREHOUSE_ML_HANDOFF.md): hướng dẫn cho nhánh ML.
- [PostgreSQL Dashboard Handoff](docs/POSTGRES_WAREHOUSE_DASHBOARD_HANDOFF.md): hướng dẫn cho nhánh dashboard.
- [Progress](PROGRESS.md): tiến độ và trạng thái dự án.

## 12. Lưu Ý Khi Làm Việc Nhóm

- Không commit file runtime như `.env`, log, checkpoint hoặc Docker volumes.
- Không chạy lệnh phá dữ liệu như `docker compose down -v` hoặc `make clean-all` nếu chưa thống nhất với nhóm.
- Nếu làm trên VM dùng chung, nên tạo branch riêng trước khi sửa code:

```bash
git fetch
git checkout -b feature/<task-name>
```

## 13. Thành Viên Nhóm

| MSV | Họ và tên |
|---|---|
| 23020551 | Giang Tuấn Minh |
| 23020567 | Lê Văn Tâm |
| 23020507 | Đinh Văn An |
| 23021551 | Nguyễn Quang Hiếu |
| 23021477 | Nguyễn Văn Biển |
