# MetroPulse

MetroPulse là nền tảng Big Data phân tích và dự báo nhu cầu di chuyển đô thị tại NYC, sử dụng dữ liệu NYC Taxi trip data kết hợp với dữ liệu thời tiết lịch sử.

## Mục Tiêu

- Ingest dữ liệu taxi và weather qua Kafka.
- Lưu dữ liệu raw vào Bronze layer trên MinIO.
- Chuẩn hóa, quality check, xử lý null và enrich dữ liệu ở Silver layer bằng Spark.
- Chuẩn bị dữ liệu Gold phục vụ Power BI dashboard và ML forecasting.

## Kiến Trúc

MetroPulse đi theo Medallion Lakehouse Architecture:

```text
Kafka -> Bronze -> Silver -> Gold
```

Các thành phần chính:

- Kafka: event transport layer.
- Spark 3.5.1: xử lý batch/streaming và ETL phân tán.
- MinIO: S3-compatible object storage cho lakehouse.
- Airflow: orchestration layer cho pipeline jobs.
- Docker Compose: triển khai single-host trên GCP VM.

## Trạng Thái Hiện Tại

- Bronze Layer: đã có pipeline Kafka -> MinIO.
- Silver Layer: đã hoàn thành transform, core schema, quality check và clean dataset.
- Airflow: đã có DAG `metropulse_silver_pipeline` để orchestrate Silver jobs.
- Gold Layer: bước tiếp theo, đọc từ `s3a://silver/taxi_weather_trips_clean/`.

## Tài Liệu Chính

- [Setup Guide](docs/SETUP_GUIDE.md): hướng dẫn SSH vào VM, làm chung trên repo, cấu hình, chạy services và troubleshooting.
- [Progress](PROGRESS.md): trạng thái dự án, kế hoạch chạy và kết quả đã hoàn thành.
- [Makefile Guide](docs/MAKEFILE_GUIDE.md): danh sách lệnh `make` dùng trong project.

## Thành Viên Nhóm

| MSV | Họ và tên |
|---|---|
| 23020551 | Giang Tuấn Minh |
| 23020567 | Lê Văn Tâm |
| 23020507 | Đinh Văn An |
| 23021551 | Nguyễn Quang Hiếu |
| 23021477 | Nguyễn Văn Biển |
