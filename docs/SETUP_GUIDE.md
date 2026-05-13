# MetroPulse Setup Guide

Tài liệu này hướng dẫn người mới clone repo, SSH vào VM, cấu hình `.env`, khởi động services và chạy pipeline MetroPulse.

## 1. Môi Trường Hiện Tại

| Hạng mục | Giá trị |
|---|---|
| Platform | GCP VM |
| Machine type | e2-standard-16 |
| Resource | 16 vCPU / 64GB RAM |
| Deployment | Docker Compose single-host |
| Project path trên VM | `/home/verzol/metropulse` |
| Spark | 3.5.1, 1 master + 2 workers |
| Storage | MinIO S3-compatible object storage |
| Orchestration | Airflow 2.9.3 |

Khuyến nghị disk:

```text
Tối thiểu để test: 100GB
Khuyến nghị cho Gold/ML: 150GB-200GB
```

## 2. SSH Vào VM

Từ máy cá nhân:

```bash
ssh <vm-user>@<VM_EXTERNAL_IP>
cd /home/verzol/metropulse
```

Nếu muốn mở UI trên browser máy cá nhân, dùng SSH tunnel:

```bash
ssh \
  -L 8088:localhost:8088 \
  -L 9001:localhost:9001 \
  -L 9090:localhost:9090 \
  -L 8080:localhost:8080 \
  -L 8081:localhost:8081 \
  -L 8082:localhost:8082 \
  <vm-user>@<VM_EXTERNAL_IP>
```

Sau đó mở:

| Service | URL local qua tunnel |
|---|---|
| Airflow | `http://localhost:8088` |
| MinIO Console | `http://localhost:9001` |
| Kafdrop | `http://localhost:9090` |
| Spark Master | `http://localhost:8080` |
| Spark Worker 1 | `http://localhost:8081` |
| Spark Worker 2 | `http://localhost:8082` |

Có thể truy cập trực tiếp bằng `http://<VM_EXTERNAL_IP>:<PORT>` nếu GCP firewall đã mở port tương ứng.

## 3. Clone Và Cài Đặt

```bash
git clone <repo-url>
cd metropulse
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Hoặc:

```bash
make install
```

## 4. Cấu Hình `.env`

Tạo file local:

```bash
cp .env.example .env
```

Cập nhật secrets và giá trị host-specific:

```bash
MINIO_ACCESS_KEY=<your_minio_access_key>
MINIO_SECRET_KEY=<your_minio_secret_key>
AIRFLOW_ADMIN_PASSWORD=<your_airflow_password>
AIRFLOW_WEBSERVER_SECRET_KEY=<random_secret>
GCP_EXTERNAL_IP=<VM_EXTERNAL_IP>
```

Thêm UID/GID để Airflow có quyền ghi log và gọi Docker:

```bash
sed -i '/^AIRFLOW_UID=/d;/^DOCKER_GID=/d;/^DOCKER_SOCKET_GID=/d' .env
echo "AIRFLOW_UID=$(id -u)" >> .env
echo "DOCKER_GID=$(getent group docker | cut -d: -f3)" >> .env
echo "DOCKER_SOCKET_GID=$(stat -c '%g' /var/run/docker.sock)" >> .env
```

Các path chính trong MinIO:

```text
s3a://bronze/yellow_taxi/
s3a://bronze/green_taxi/
s3a://bronze/weather/
s3a://silver/hourly_weather/
s3a://silver/taxi_weather_trips/
s3a://silver/taxi_weather_trips_core/
s3a://silver/taxi_weather_trips_clean/
s3a://silver/quality_reports/silver_core_quality/latest/
```

Không commit `.env`.

## 5. Khởi Động Services

```bash
make start
make airflow-init
make airflow-up
make status
```

Các service chính:

| Service | Port | Mục đích |
|---|---:|---|
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092, 29092 | Event transport |
| Kafdrop | 9090 | Kafka UI |
| MinIO | 9000, 9001 | Lakehouse object storage |
| Spark Master | 7077, 8080 | Spark cluster master + UI |
| Spark Worker 1 | 8081 | Spark worker |
| Spark Worker 2 | 8082 | Spark worker |
| Airflow Webserver | 8088 | Airflow UI |
| Airflow Scheduler | internal | DAG scheduler |
| Airflow Postgres | internal | Airflow metadata DB |

Airflow login lấy từ `.env`:

```text
AIRFLOW_ADMIN_USERNAME
AIRFLOW_ADMIN_PASSWORD
```

## 6. Tải Dữ Liệu

Tải NYC taxi parquet data:

```bash
chmod +x download_data.sh
./download_data.sh
```

Full dataset hiện tại dùng 48 parquet files cho yellow/green taxi giai đoạn 2023-2024.

## 7. Chạy Pipeline

Weather producer:

```bash
make weather
```

Taxi producer:

```bash
make producer
```

Bronze:

```bash
make bronze
```

Silver:

```bash
make silver
make silver-core
make silver-quality
make silver-clean
```

Nếu đã có sẵn Silver Core trong MinIO, có thể chạy lại Phase 2:

```bash
make silver-quality
make silver-clean
```

## 8. Chạy Silver Qua Airflow

Mở:

```text
http://localhost:8088
```

Trigger DAG:

```text
metropulse_silver_pipeline
```

Flow hiện tại:

```text
silver -> silver-core -> silver-quality -> silver-clean
```

Airflow chỉ orchestrate. Spark vẫn xử lý dữ liệu lớn.

## 9. Trạng Thái Silver Hiện Tại

Silver Layer đã hoàn thành Phase 2.

Output hiện có:

```text
s3a://silver/hourly_weather/
s3a://silver/taxi_weather_trips/
s3a://silver/taxi_weather_trips_core/
s3a://silver/taxi_weather_trips_clean/
s3a://silver/quality_reports/silver_core_quality/latest/
```

Quality report mới nhất:

```text
_SUCCESS
0 failed checks
```

Gold nên đọc từ:

```text
s3a://silver/taxi_weather_trips_clean/
```

Với bảng analytics/ML sạch, lọc:

```text
is_gold_candidate = true
```

## 10. Runtime State

### Producer checkpoint

File local:

```text
.producer_checkpoint.json
```

File này giúp producer biết đã gửi tới đâu. Không commit file này. Chỉ xóa khi muốn replay producer từ đầu.

Tác động khi xóa:

- Kafka có thể nhận lại dữ liệu cũ;
- Bronze/Silver có thể phải xử lý duplicate/reprocess;
- chỉ nên dùng khi rebuild pipeline có chủ đích.

### MinIO checkpoints

Không xóa checkpoint trong MinIO nếu không muốn reset Structured Streaming:

```text
s3a://bronze/checkpoints/
```

## 11. Troubleshooting

Kiểm tra services:

```bash
docker compose ps
make status
```

Xem logs:

```bash
docker compose logs <service> --tail=100
make logs
```

Airflow không gọi được Docker:

```bash
getent group docker | cut -d: -f3
stat -c '%g' /var/run/docker.sock
docker compose exec airflow-webserver id
```

Sau khi sửa `.env`, recreate Airflow:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

Spark job lỗi:

```bash
docker compose logs spark-master --tail=200
```

Kiểm tra disk:

```bash
df -h .
docker system df
```

Không chạy `docker volume prune` nếu còn cần dữ liệu MinIO/Airflow Postgres.

## 12. Cleanup

Dừng service nhưng giữ data:

```bash
make stop
```

Xóa toàn bộ Docker volumes, gồm MinIO data:

```bash
make clean-all
```

`make clean-all` có tính phá hủy, chỉ dùng khi muốn reset môi trường.

## 13. Tài Liệu Liên Quan

- [Makefile Guide](MAKEFILE_GUIDE.md)
- [Progress](../PROGRESS.md)
