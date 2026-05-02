# Hướng Dẫn Hạ Tầng Metropulse

## Thông Tin Máy Chủ

| Thông tin | Chi tiết |
|-----------|---------|
| OS | Ubuntu 26.04 LTS |
| Cấu hình | 4 vCPU, 16GB RAM (e2-standard-4) |
| Region | asia-southeast1 (Singapore) |
| Username | verzol |
| Thư mục | /home/verzol/metropulse |

## Đã Setup

6 Docker services:
- Zookeeper (port 2181)
- Kafka (port 9092)
- MinIO (ports 9000, 9001)
- Spark Master (port 8080, 7077)
- Spark Worker (port 8081)
- Kafdrop (port 9090)

Docker images đã pull: wurstmeister/kafka, zookeeper, apache/spark:3.5.0, minio, kafdrop

## Cách Chạy

Bước 1: Start VM trên GCP Console

Bước 2: SSH vào máy
```bash
ssh verzol@<EXTERNAL_IP>
```

Bước 3: Khởi động
```bash
cd ~/metropulse
docker compose up -d
```

## Cách Kiểm Tra

Xem containers chạy:
```bash
docker ps
```

Xem logs của một service:
```bash
docker compose logs kafka --tail=50
```

Xem trạng thái tất cả services:
```bash
docker compose ps -a
```

## Truy Cập Services

| Service | URL |
|---------|-----|
| Kafdrop (Kafka UI) | http://[EXTERNAL_IP]:9090 |
| Spark Master | http://[EXTERNAL_IP]:8080 |
| MinIO Console | http://[EXTERNAL_IP]:9001 |
| Kafka Broker | [EXTERNAL_IP]:9092 |

MinIO login: admin / metropulse2026

## Dừng Services

```bash
docker compose stop       # Dừng (giữ data)
docker compose down -v    # Xóa tất cả (mất data)
docker compose restart    # Restart
```

## Troubleshooting

Không vào được services: Kiểm tra GCP Firewall đã mở ports chưa

Services không start: `docker compose logs` để xem chi tiết

Kafka lỗi: Kiểm tra Zookeeper: `docker compose logs zookeeper`
