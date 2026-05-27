# Hướng Dẫn Bắt Đầu Cho Bạn Làm Dashboard

## Bạn Được Cấp Gì

Người quản lý dự án sẽ gửi riêng cho bạn:

```text
DASHBOARD_DB_USER=<username>
DASHBOARD_DB_PASSWORD=<password>
VM_USER=<ssh username>
VM_IP=<địa chỉ VM>
```

Bạn chỉ cần đọc các bảng dashboard trong PostgreSQL. Không cần chạy Airflow, Spark, Kafka hay MinIO.

## 1. Chọn Cách Kết Nối

### Cách A: Làm Python, Streamlit Hoặc Tool Chạy Trên VM

SSH vào VM:

```bash
ssh <VM_USER>@<VM_IP>
```

Khai báo credential trong terminal trên VM:

```bash
export DASHBOARD_DB_USER='<username>'
export DASHBOARD_DB_PASSWORD='<password>'
```

Database connection:

```text
Host: 127.0.0.1
Port: 5433
Database: metropulse_dw
Username: giá trị DASHBOARD_DB_USER
Password: giá trị DASHBOARD_DB_PASSWORD
```

### Cách B: Làm Power BI Desktop Trên Máy Cá Nhân

Mở SSH tunnel và giữ terminal này đang chạy:

```bash
ssh -L 5433:127.0.0.1:5433 <VM_USER>@<VM_IP>
```

Trong Power BI, chọn connector `PostgreSQL database` và nhập:

```text
Server: localhost:5433
Database: metropulse_dw
Username: tài khoản dashboard được cấp
Password: password dashboard được cấp
Data connectivity mode: Import
```

Dùng `Import` là phù hợp với ba bảng aggregate nhỏ hiện tại.

## 2. Test Kết Nối Database

Nếu dùng Power BI Desktop, kết nối thành công khi Power BI hiển thị ba bảng trong schema `mart`.

Nếu làm Python/Streamlit trên VM, cài driver:

```bash
pip install pandas sqlalchemy psycopg2-binary
```

Sau đó test trong Python:

```python
import os
import pandas as pd
from sqlalchemy import URL, create_engine

engine = create_engine(
    URL.create(
        "postgresql+psycopg2",
        username=os.environ["DASHBOARD_DB_USER"],
        password=os.environ["DASHBOARD_DB_PASSWORD"],
        host="127.0.0.1",
        port=5433,
        database="metropulse_dw",
    )
)

check = pd.read_sql(
    "SELECT COUNT(*) AS rows, SUM(total_demand) AS total_demand "
    "FROM mart.dashboard_hourly_demand_kpi",
    engine,
)
print(check)
```

Kết quả hiện tại sẽ gần như:

```text
rows = 17542
total_demand = 78272751
```

Nếu không connect được, gửi lỗi cho người quản lý dự án. Không tự sửa database.

## 3. Ba Bảng Để Làm Dashboard

| Bảng | Dùng Để Vẽ Gì | Số Dòng Hiện Tại |
|---|---|---:|
| `mart.dashboard_hourly_demand_kpi` | Nhu cầu theo giờ, trend, ảnh hưởng thời tiết | `17,542` |
| `mart.dashboard_zone_summary` | Bản đồ/borough/zone có nhu cầu cao | `263` |
| `mart.dashboard_payment_tip_summary` | Fare/tip/payment theo tháng | `160` |

## 4. Query Mẫu

### KPI Theo Thời Gian

```sql
SELECT
    pickup_hour,
    total_demand,
    active_zones,
    avg_temperature_f,
    avg_precipitation_mm
FROM mart.dashboard_hourly_demand_kpi
ORDER BY pickup_hour;
```

### Demand Theo Borough/Zone

```sql
SELECT
    pickup_borough,
    pickup_zone,
    total_demand,
    avg_hourly_demand,
    max_hourly_demand
FROM mart.dashboard_zone_summary
ORDER BY total_demand DESC;
```

### Payment Và Tip Theo Tháng

```sql
SELECT
    pickup_year_month,
    payment_type,
    trip_count,
    avg_fare_amount,
    avg_tip_amount,
    avg_tip_percent
FROM mart.dashboard_payment_tip_summary
ORDER BY pickup_year_month, payment_type;
```

## 5. Bắt Đầu Build Dashboard

Flow ngắn gọn:

```text
Connect PostgreSQL
-> load 3 bảng mart.dashboard_*
-> tạo relationships/measures nếu cần
-> vẽ KPI cards, trend chart, zone map và payment/tip charts
```

Gợi ý trang dashboard đầu tiên:

| Visual | Nguồn Dữ Liệu |
|---|---|
| Total demand KPI | `dashboard_hourly_demand_kpi` |
| Hourly/monthly demand trend | `dashboard_hourly_demand_kpi` |
| Demand theo borough/zone | `dashboard_zone_summary` |
| Zone ranking hoặc map | `dashboard_zone_summary` |
| Tip/payment trend | `dashboard_payment_tip_summary` |

## Không Làm Những Việc Này

- Không đọc hoặc chia sẻ file `.env`.
- Không dùng connection `MetroPulse Warehouse` của owner.
- Không chạy `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`.
- Không query schema `staging`.
- Không tự refresh pipeline khi chưa thống nhất với người quản lý dự án.
