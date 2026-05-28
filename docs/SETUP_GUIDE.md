# MetroPulse Setup Guide

Tài liệu này hướng dẫn người mới SSH vào VM, làm chung trên repo đang chạy, cấu hình môi trường khi cần, kiểm tra Docker services và chạy pipeline MetroPulse.

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
| Serving Warehouse | PostgreSQL riêng cho ML/dashboard publication |
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

Với PostgreSQL Warehouse và pgAdmin, dùng SSH tunnel để thành viên truy cập cùng service trên VM mà không mở port database/UI quản trị ra public internet:

```bash
ssh \
  -L 5433:localhost:5433 \
  -L 5050:localhost:5050 \
  <vm-user>@<VM_EXTERNAL_IP>
```

Sau đó mở:

| Service | URL local qua tunnel |
|---|---|
| PostgreSQL Warehouse | `localhost:5433` |
| pgAdmin | `http://localhost:5050` |

Public IP của VM vẫn được dùng cho SSH (`port 22`). Các UI đang publish public sẵn của hệ thống prototype như Airflow/MinIO/Spark cần được kiểm soát ở GCP firewall; PostgreSQL và pgAdmin được khóa localhost theo phương án đã chốt.

## 3. Làm Chung Trên VM

Phương án hiện tại của nhóm:

```text
Tất cả thành viên làm chung trên VM.
Không clone thêm repo mới trên VM.
Repo dùng chung: /home/verzol/metropulse
Mỗi người dùng SSH user riêng và branch Git riêng.
```

Lý do chọn phương án này:

- MinIO data, Docker volumes, Airflow metadata và checkpoint đã nằm trên VM hiện tại.
- Không cần copy lại `.env`, raw data, MinIO volumes hoặc checkpoint sang workspace khác.
- Tránh chạy trùng nhiều Docker Compose project gây tốn disk/RAM.
- Cả nhóm kiểm tra cùng một pipeline state.

### 3.1 Việc Nhóm Trưởng Cần Làm

Với mỗi thành viên mới, nhóm trưởng cần:

1. Nhận public SSH key của thành viên.
2. Add SSH key vào GCP VM.
3. Cho user đó quyền đi vào project folder.
4. Cho user đó quyền chạy Docker.
5. Add thành viên vào GitHub repo nếu repo private.

Thành viên tạo SSH key trên máy cá nhân nếu chưa có:

```bash
ssh-keygen -t ed25519 -C "member_name"
cat ~/.ssh/id_ed25519.pub
```

Nhóm trưởng add public key vào GCP:

```text
GCP Console
-> Compute Engine
-> VM instances
-> chọn VM
-> Edit
-> SSH Keys
-> Add item
-> paste public key
-> Save
```

Sau khi thành viên SSH vào được, nhóm trưởng cấp quyền trên VM. Thay `<member_user>` bằng username thật của thành viên:

```bash
sudo chmod o+x /home/verzol
sudo usermod -aG docker <member_user>
```

Nếu thành viên cần sửa file trong repo dùng chung:

```bash
sudo apt install -y acl
sudo setfacl -R -m u:<member_user>:rwx /home/verzol/metropulse
sudo setfacl -R -d -m u:<member_user>:rwx /home/verzol/metropulse
```

Sau khi thêm vào group Docker, thành viên phải logout rồi SSH lại:

```bash
exit
ssh <member_user>@<VM_EXTERNAL_IP>
```

### 3.2 Việc Thành Viên Cần Làm Lần Đầu

SSH vào VM:

```bash
ssh <member_user>@<VM_EXTERNAL_IP>
```

Vào repo dùng chung:

```bash
cd /home/verzol/metropulse
```

Nếu Git báo `detected dubious ownership`, chạy:

```bash
git config --global --add safe.directory /home/verzol/metropulse
```

Kiểm tra quyền và trạng thái:

```bash
whoami
pwd
git status
docker ps
docker compose ps
make status
```

Nếu `docker ps` báo permission denied, báo nhóm trưởng kiểm tra lại Docker group rồi logout/login SSH lại.

### 3.3 Quy Tắc Làm Việc Chung

Không sửa trực tiếp trên branch chính nếu không cần. Mỗi người tạo branch riêng:

```bash
git fetch
git checkout -b feature/<short-task-name>
```

Ví dụ:

```bash
git checkout -b feature/gold-hourly-demand
```

Trước khi sửa code:

```bash
git status
git pull
```

Sau khi sửa:

```bash
git status
git diff
```

Không commit các file runtime:

```text
.env
.producer_checkpoint.json
airflow/logs/
data/raw/
__pycache__/
MinIO/Docker volumes
```

Không tự ý chạy các lệnh destructive:

```bash
make clean-all
docker compose down -v
docker volume prune
rm .producer_checkpoint.json
```

Các lệnh kiểm tra an toàn:

```bash
docker compose ps
make status
make airflow-dags
docker compose logs airflow-webserver --tail=50
docker compose logs spark-master --tail=50
df -h .
docker system df
```

Các lệnh pipeline nên báo nhóm trước khi chạy vì có thể tốn thời gian hoặc ghi dữ liệu:

```bash
make producer
make bronze
make silver
make silver-core
make silver-clean
```

Lệnh tương đối an toàn để kiểm tra Silver hiện tại:

```bash
make silver-quality
```

### 3.4 Mở pgAdmin Trên Máy Cá Nhân Của Thành Viên

Mỗi thành viên SSH tunnel vào cùng VM:

```bash
ssh \
  -L 5050:localhost:5050 \
  -L 5433:localhost:5433 \
  <member_user>@<VM_EXTERNAL_IP>
```

Sau đó mở browser hoặc database client:

| Service | URL |
|---|---|
| pgAdmin | `http://localhost:5050` |
| PostgreSQL client | `localhost:5433` |

Mọi thành viên vẫn đọc cùng PostgreSQL volume trên VM; tunnel chỉ bảo vệ đường truy cập. Thông tin đăng nhập hiện tại lấy từ `.env` trên VM. Không đưa `.env` lên GitHub.

### 3.5 Checklist Lần Đầu Cho Thành Viên

```bash
ssh <member_user>@<VM_EXTERNAL_IP>
cd /home/verzol/metropulse
git config --global --add safe.directory /home/verzol/metropulse
git status
docker ps
docker compose ps
make status
make airflow-dags
```

Nếu tất cả lệnh trên chạy được, thành viên đã sẵn sàng làm việc trên VM chung.

## 4. Clone Và Cài Đặt Khi Setup VM Mới

Không dùng phần này nếu đang làm trên VM chung hiện tại. Phần này chỉ dành cho trường hợp setup một máy mới từ đầu.

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

## 5. Cấu Hình `.env`

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
WAREHOUSE_POSTGRES_PASSWORD=<strong_warehouse_password>
WAREHOUSE_ML_READER_USER=metropulse_ml_reader
WAREHOUSE_ML_READER_PASSWORD=<strong_ml_reader_password>
WAREHOUSE_DASHBOARD_READER_USER=metropulse_dashboard_reader
WAREHOUSE_DASHBOARD_READER_PASSWORD=<strong_dashboard_reader_password>
PGADMIN_DEFAULT_EMAIL=<your_pgadmin_email>
PGADMIN_DEFAULT_PASSWORD=<strong_pgadmin_password>
GCP_EXTERNAL_IP=<VM_EXTERNAL_IP>
```

Thêm UID/GID để Airflow có quyền ghi log và gọi Docker:

```bash
sed -i '/^AIRFLOW_UID=/d;/^DOCKER_GID=/d;/^DOCKER_SOCKET_GID=/d' .env
echo "AIRFLOW_UID=$(id -u)" >> .env
echo "DOCKER_GID=$(getent group docker | cut -d: -f3)" >> .env
echo "DOCKER_SOCKET_GID=$(stat -c '%g' /var/run/docker.sock)" >> .env
chmod 600 .env
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
s3a://gold/gold_demand_features/
s3a://gold/gold_fare_tip_features/
s3a://gold/quality_reports/gold_quality/latest/
```

Không commit `.env`.

## 6. Khởi Động Services

```bash
make start
make airflow-init
make warehouse-init
make warehouse-ml-access
make warehouse-dashboard-access
make pgadmin-up
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
| PostgreSQL Warehouse | 5433 (localhost VM only) | Serving DB riêng cho ML/dashboard |
| pgAdmin | 5050 (localhost VM only) | Web UI kiểm tra PostgreSQL Warehouse qua SSH tunnel |

Airflow login lấy từ `.env`:

```text
AIRFLOW_ADMIN_USERNAME
AIRFLOW_ADMIN_PASSWORD
```

Kiểm tra warehouse foundation:

```bash
make warehouse-status
```

Warehouse schemas ban đầu:

```text
ml      - table phục vụ machine learning
mart    - tables aggregate phục vụ dashboard
audit   - lịch sử publish và validation
staging - private tables cho publication; consumer không có quyền đọc
```

`ml.gold_demand_features` phục vụ demand forecasting; `ml.gold_fare_tip_features` phục vụ fare/tip modeling; ba bảng `mart.dashboard_*` phục vụ dashboard. Serving Layer nạp các bảng từ Gold MinIO bằng Spark JDBC, promote qua transaction và ghi validation audit:

```bash
make gold-quality
make gold-dashboard
make gold-publish-serving
make warehouse-status
```

MinIO vẫn là source of truth; PostgreSQL chỉ là bản serving được publish. Xem [POSTGRES_WAREHOUSE_ML_HANDOFF.md](POSTGRES_WAREHOUSE_ML_HANDOFF.md) và [POSTGRES_WAREHOUSE_DASHBOARD_HANDOFF.md](POSTGRES_WAREHOUSE_DASHBOARD_HANDOFF.md).

`gold_fare_tip_features` là bảng trip-level lớn. Publisher dùng mặc định `2` JDBC write partitions để hạn chế concurrent insert và áp lực memory/I/O trên PostgreSQL single-host; lần full-load đầu tiên có thể chạy lâu hơn bảng demand. Khi nhóm ML chỉ cần refresh fare/tip, chạy `make gold-publish-fare-tip` thay vì refresh toàn bộ serving layer.

Cấp login read-only cho nhóm ML và dashboard:

```bash
make warehouse-ml-access
make warehouse-dashboard-access
```

Chia sẻ credential `WAREHOUSE_ML_READER_*` hoặc `WAREHOUSE_DASHBOARD_READER_*` ngoài Git cho đúng consumer; không chia sẻ tài khoản owner warehouse cho notebook/training/dashboard.

Để read-only access thực sự có ý nghĩa bảo mật, không cấp quyền đọc `.env` cho thành viên chỉ làm ML. Nếu nhóm dùng chung một Unix account trên VM, mọi người có thể đọc cùng secret; khi đó cần chuyển sang user Linux tách biệt hoặc secret management trước khi xem đây là phân quyền an toàn.

Kiểm tra warehouse bằng pgAdmin:

```bash
make pgadmin-up
```

Từ máy cá nhân, mở tunnel `ssh -L 5050:localhost:5050 -L 5433:localhost:5433 <vm-user>@<VM_EXTERNAL_IP>` rồi vào `http://localhost:5050`. Đăng nhập bằng `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` trong `.env`. Nhóm ML chọn `MetroPulse ML Read Only`; nhóm dashboard chọn `MetroPulse Dashboard Read Only`; không dùng connection owner cho consumer workloads.

pgAdmin hiện sử dụng HTTP nội bộ nhưng traffic từ máy thành viên tới VM đi qua SSH tunnel đã mã hóa. Không mở port `5050` hoặc `5433` trên GCP firewall.

## 7. Tải Dữ Liệu

Tải NYC taxi parquet data:

```bash
chmod +x download_data.sh
./download_data.sh
```

Full dataset hiện tại dùng 48 parquet files cho yellow/green taxi giai đoạn 2023-2024.

## 8. Chạy Pipeline

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

## 9. Chạy Silver Qua Airflow

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

Gold DAG `metropulse_gold_pipeline` hiện bao gồm publication sang PostgreSQL:

```text
Gold transform -> Gold quality -> Gold dashboard marts
-> initialize warehouse -> publish ML demand features -> validate ML publication
-> publish ML fare/tip features -> validate fare/tip publication
-> publish dashboard marts -> validate dashboard publication
```

Đảm bảo chạy `make warehouse-up` hoặc `make start` trước khi trigger DAG. Warehouse service được khởi động từ host; Airflow chỉ initialize SQL trên container đang hoạt động và điều phối Spark publisher/validator.

Sau khi trigger DAG thành công, kiểm tra `audit.publish_run_history` trong pgAdmin hoặc chạy `make warehouse-status`.

## 10. Trạng Thái Silver Hiện Tại

Silver Layer đã hoàn thành Phase 2.

Output hiện có:

```text
s3a://silver/hourly_weather/
s3a://silver/taxi_weather_trips/
s3a://silver/taxi_weather_trips_core/
s3a://silver/taxi_weather_trips_clean/
s3a://silver/quality_reports/silver_core_quality/latest/
```

Quality artifact đang lưu của Core snapshot:

```text
_SUCCESS
0 failed checks
```

Artifact này áp dụng cho Core snapshot đã ghi; EDA hiện tại đã phát hiện Core không cùng row-count snapshot với `taxi_weather_trips` mới hơn. Khi handoff Gold chính thức, chạy lại Silver/Core quality theo cùng một pipeline run.

Gold nên đọc từ hai nguồn:

```text
s3a://silver/hourly_weather/
s3a://silver/taxi_weather_trips_core/
```

`hourly_weather` giữ weather dimension theo giờ; `taxi_weather_trips_core` giữ trip-level facts và weather features đã cast gọn. Với bảng ML hoặc KPI cần loại outlier, áp dụng:

```text
is_outlier_trip = false
is_valid_distance = true
is_valid_fare = true
is_valid_total_amount = true
```

Các field vận hành còn nullable trong Core phải được impute hoặc giữ missing indicator trong Gold transform nếu được dùng làm feature.

## 11. Runtime State

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

## 12. Troubleshooting

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

Không chạy `docker volume prune` nếu còn cần dữ liệu MinIO, Airflow Postgres, PostgreSQL Warehouse hoặc cấu hình pgAdmin.

## 13. Cleanup

Dừng service nhưng giữ data:

```bash
make stop
```

Xóa toàn bộ Docker volumes, gồm MinIO data, PostgreSQL Warehouse data và cấu hình pgAdmin:

```bash
make clean-all
```

`make clean-all` có tính phá hủy, chỉ dùng khi muốn reset môi trường.

## 14. Tài Liệu Liên Quan

- [Makefile Guide](MAKEFILE_GUIDE.md)
- [Progress](../PROGRESS.md)
