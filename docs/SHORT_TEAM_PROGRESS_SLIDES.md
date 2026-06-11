# MetroPulse Progress Deck

## Slide 1 — Executive Snapshot

**MetroPulse: Big Data Platform for NYC Mobility Demand Analytics**

**One-line message:** Nhóm đã xây dựng được một pipeline dữ liệu end-to-end từ ingestion đến serving cho ML và dashboard.

| Capability | Current Result |
|---|---|
| Data pipeline | Kafka → Bronze → Silver → Gold → PostgreSQL |
| Processing engine | Apache Spark 3.5.1 trên Docker Compose |
| Lakehouse storage | MinIO với Bronze, Silver, Gold buckets |
| Serving layer | PostgreSQL Warehouse cho ML và dashboard |
| Orchestration | Airflow DAGs cho Silver và Gold pipeline |

**Visual gợi ý:** Vẽ flow ngang 5 khối: `Kafka` → `Bronze` → `Silver` → `Gold` → `PostgreSQL / ML / Dashboard`.

---

## Slide 2 — Data Ingestion & Bronze Layer

**Message:** Nhóm đã hoàn thành lớp thu thập dữ liệu raw theo đúng nguyên tắc Bronze: lưu nguyên trạng, có checkpoint, không làm sạch sớm.

| Component | What Was Built |
|---|---|
| Taxi producer | Đọc Yellow/Green Taxi data và gửi JSON payload vào Kafka |
| Weather producer | Lấy historical weather từ Open-Meteo và stream vào Kafka |
| Kafka topics | `nyc_taxi_yellow`, `nyc_taxi_green`, `weather_stream` |
| Bronze storage | Spark Structured Streaming ghi raw payload xuống MinIO |
| Fault tolerance | Checkpoint cho streaming jobs trên `s3a://bronze/checkpoints/` |

**Key point để nói:** Bronze giữ source fidelity, nên các bước sau có thể kiểm tra lại lineage và tái xử lý khi cần.

**Visual gợi ý:** 3 input cards: Yellow Taxi, Green Taxi, Weather → Kafka → Bronze bucket.

---

## Slide 3 — Silver & Gold Processing

**Message:** Nhóm đã biến dữ liệu raw thành dữ liệu sạch, enriched và sẵn sàng cho phân tích.

| Layer | Main Work |
|---|---|
| Silver | Chuẩn hóa schema, ép kiểu, xử lý null, deduplicate |
| Silver enrichment | Join taxi trips với hourly weather theo `pickup_hour` |
| Timezone policy | Chuẩn hóa theo `America/New_York` |
| Gold demand | Feature table cho demand forecasting theo zone × hour |
| Gold fare/tip | Trip-level feature table cho fare và tip modeling |
| Dashboard marts | Aggregate sẵn để dashboard query nhanh hơn |

**KPI nổi bật:**

| Metric | Value |
|---|---:|
| Silver Clean rows | 80.9M+ |
| Gold demand features | 1,977,231 rows |
| Gold fare/tip features | 78.0M+ rows |
| Gold quality checks | 75/75 pass |

**Visual gợi ý:** Chia slide thành 2 cột: bên trái `Silver: Clean + Enrich`, bên phải `Gold: ML Features + Dashboard Marts`.

---

## Slide 4 — Serving, Orchestration & Quality

**Message:** Nhóm không chỉ tạo file Parquet, mà còn xây được lớp serving có kiểm soát để ML/dashboard sử dụng ổn định.

| Area | Current Result |
|---|---|
| PostgreSQL ML serving | `ml.gold_demand_features`, `ml.gold_fare_tip_features` |
| Dashboard serving | `mart.dashboard_hourly_demand_kpi`, `mart.dashboard_zone_summary`, `mart.dashboard_payment_tip_summary` |
| Validation | Source-target checks khi publish từ Gold MinIO sang PostgreSQL |
| Access control | Read-only roles riêng cho ML và dashboard users |
| Airflow | DAG Silver và Gold điều phối Spark jobs, quality checks, publish jobs |
| pgAdmin | UI kiểm tra warehouse qua SSH tunnel, không expose public DB port |

**Operational tradeoff:** MinIO vẫn là source of truth; PostgreSQL chỉ là serving layer để consumer query nhanh và an toàn hơn.

**Visual gợi ý:** Vẽ `Gold Parquet` ở giữa, tách ra 2 nhánh: `ML tables` và `Dashboard marts`, kèm icon validation.

---

## Slide 5 — ML, Dashboard & Next Steps

**Message:** Dữ liệu Gold đã được handoff sang các workload tiêu thụ thực tế: forecasting model và dashboard.

| Consumer | What Was Prepared |
|---|---|
| Demand prediction | XGBoost pipeline với zone-hour features |
| Feature engineering | Lag features 1h, 24h, 168h; weather và time features |
| Fare/tip modeling | Trip-level Gold dataset cho bài toán mở rộng |
| Dashboard | Streamlit app/API đọc từ dashboard marts đã aggregate |

**Current outcome:**

- Có model artifacts và metrics logs trong `ml/`.
- Dashboard không query trực tiếp bảng trip-level lớn, giảm áp lực lên warehouse.
- Gold pipeline đã có quality gate trước khi publish cho consumer.

**Next steps:**

1. Chạy lại Silver end-to-end để đồng bộ lineage final snapshot.
2. Benchmark incremental refresh cho bảng fare/tip lớn.
3. Bổ sung zero-demand grid nếu mô hình forecasting cần học cả giờ không có chuyến.

**Closing line:** MetroPulse hiện đã đạt mức prototype end-to-end: ingestion, lakehouse processing, quality validation, serving, ML và dashboard handoff.
