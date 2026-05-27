# MetroPulse

MetroPulse là nền tảng Big Data phân tích và dự báo nhu cầu di chuyển đô thị tại NYC, sử dụng dữ liệu NYC Taxi trip data kết hợp với dữ liệu thời tiết lịch sử.

## Mục Tiêu

- Ingest dữ liệu taxi và weather qua Kafka.
- Lưu dữ liệu raw vào Bronze layer trên MinIO.
- Chuẩn hóa, quality check, xử lý null và enrich dữ liệu ở Silver layer bằng Spark.
- Chuẩn bị dữ liệu Gold phục vụ Power BI dashboard và ML forecasting.

## Kiến Trúc

MetroPulse đi theo Medallion Lakehouse Architecture, với PostgreSQL làm serving warehouse cho consumer:

```text
Kafka -> Bronze/MinIO -> Silver/MinIO -> Gold/MinIO -> PostgreSQL DW -> ML/Dashboard
```

Các thành phần chính:

- Kafka: event transport layer.
- Spark 3.5.1: xử lý batch/streaming và ETL phân tán.
- MinIO: S3-compatible object storage cho lakehouse.
- PostgreSQL: Data Warehouse / serving layer cho ML và dashboard tables đã publish.
- Airflow: orchestration layer cho pipeline jobs.
- Docker Compose: triển khai single-host trên GCP VM.

## Trạng Thái Hiện Tại

- Bronze Layer: đã có pipeline Kafka -> MinIO.
- Silver Layer: đã hoàn thành transform, core schema, quality check và clean dataset.
- Gold Layer: đã hoàn thành ML-ready datasets, quality checks và dashboard marts trên MinIO.
- PostgreSQL Warehouse: `ml.gold_demand_features` và ba bảng `mart.dashboard_*` đã được publish, validate để phục vụ ML/Dashboard.
- Consumer access: đã provision login read-only riêng qua roles `ml_reader` và `dashboard_reader`; staging không mở cho consumer.
- pgAdmin: UI quản trị PostgreSQL đã cấu hình để kiểm tra warehouse qua SSH tunnel vào VM.
- Airflow: DAG Gold đã gồm publication/validation PostgreSQL cho cả ML và dashboard serving.

## Tài Liệu Chính

- [Setup Guide](docs/SETUP_GUIDE.md): hướng dẫn SSH vào VM, làm chung trên repo, cấu hình, chạy services và troubleshooting.
- [Progress](PROGRESS.md): trạng thái dự án, kế hoạch chạy và kết quả đã hoàn thành.
- [Makefile Guide](docs/MAKEFILE_GUIDE.md): danh sách lệnh `make` dùng trong project.
- [PostgreSQL ML Handoff](docs/POSTGRES_WAREHOUSE_ML_HANDOFF.md): contract và hướng dẫn truy cập dataset cho nhóm ML.
- [PostgreSQL Dashboard Handoff](docs/POSTGRES_WAREHOUSE_DASHBOARD_HANDOFF.md): contract các mart và hướng dẫn kết nối dashboard read-only.

## Thành Viên Nhóm

| MSV | Họ và tên |
|---|---|
| 23020551 | Giang Tuấn Minh |
| 23020567 | Lê Văn Tâm |
| 23020507 | Đinh Văn An |
| 23021551 | Nguyễn Quang Hiếu |
| 23021477 | Nguyễn Văn Biển |
