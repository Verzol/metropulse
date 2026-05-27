# Hướng Dẫn Bắt Đầu Cho Bạn Làm ML

## Bạn Được Cấp Gì

Người quản lý dự án sẽ gửi riêng cho bạn:

```text
ML_DB_USER=<username>
ML_DB_PASSWORD=<password>
VM_USER=<ssh username>
VM_IP=<địa chỉ VM>
```

Bạn chỉ cần đọc dữ liệu trong PostgreSQL để làm model. Không cần chạy Airflow, Spark, Kafka hay MinIO.

## 1. SSH Vào VM

Trên máy cá nhân:

```bash
ssh <VM_USER>@<VM_IP>
```

Sau khi vào VM, khai báo tài khoản database đã được cấp:

```bash
export ML_DB_USER='<username>'
export ML_DB_PASSWORD='<password>'
```

## 2. Cài Thư Viện Và Test Kết Nối

Trong Python environment của bạn trên VM:

```bash
pip install pandas sqlalchemy psycopg2-binary
```

Tạo file test nhanh hoặc chạy cell đầu tiên trong notebook:

```python
import os
import pandas as pd
from sqlalchemy import URL, create_engine

engine = create_engine(
    URL.create(
        "postgresql+psycopg2",
        username=os.environ["ML_DB_USER"],
        password=os.environ["ML_DB_PASSWORD"],
        host="127.0.0.1",
        port=5433,
        database="metropulse_dw",
    )
)

check = pd.read_sql(
    "SELECT COUNT(*) AS rows, SUM(demand) AS total_demand FROM ml.gold_demand_features",
    engine,
)
print(check)
```

Nếu kết nối thành công, kết quả hiện tại sẽ gần như:

```text
rows = 1977231
total_demand = 78272751
```

Nếu không connect được, gửi lỗi cho người quản lý dự án. Không tự sửa database.

## 3. Bảng Dữ Liệu Để Làm ML

Bạn đọc bảng:

```sql
ml.gold_demand_features
```

Mỗi dòng là nhu cầu taxi của một pickup zone trong một giờ.

| Cột | Ý nghĩa |
|---|---|
| `pu_location_id` | ID pickup zone |
| `pickup_hour` | Mốc thời gian theo giờ New York |
| `demand` | Số chuyến, target cần dự báo |
| `hour` | Giờ trong ngày |
| `day_of_week` | Thứ trong tuần |
| `month` | Tháng |
| `temperature_f` | Nhiệt độ |
| `precipitation_mm` | Lượng mưa |
| `pickup_year_month` | Tháng dữ liệu dạng `YYYY-MM` |
| `gold_processed_timestamp` | Snapshot dữ liệu đang dùng |

Lưu ý: bảng này chỉ có các `zone-hour` có `demand > 0`. Nếu model cần cả các giờ không có chuyến, trao đổi lại với người quản lý data.

## 4. Query Mẫu

Thử lấy một tháng dữ liệu:

```sql
SELECT
    pu_location_id,
    pickup_hour,
    demand,
    hour,
    day_of_week,
    month,
    temperature_f,
    precipitation_mm,
    pickup_year_month
FROM ml.gold_demand_features
WHERE pickup_hour >= TIMESTAMP '2024-01-01 00:00:00'
  AND pickup_hour < TIMESTAMP '2024-02-01 00:00:00'
ORDER BY pickup_hour, pu_location_id;
```

## 5. Đọc Dữ Liệu Bằng Python

Trong notebook hoặc script:

```python
import os
import pandas as pd
from sqlalchemy import URL, create_engine

engine = create_engine(
    URL.create(
        "postgresql+psycopg2",
        username=os.environ["ML_DB_USER"],
        password=os.environ["ML_DB_PASSWORD"],
        host="127.0.0.1",
        port=5433,
        database="metropulse_dw",
    )
)

query = """
SELECT
    pu_location_id,
    pickup_hour,
    demand,
    hour,
    day_of_week,
    month,
    temperature_f,
    precipitation_mm,
    pickup_year_month,
    gold_processed_timestamp
FROM ml.gold_demand_features
ORDER BY pickup_hour, pu_location_id
"""

df = pd.read_sql(query, engine)
print(df.shape)
print(df.head())
```

Dữ liệu có gần 2 triệu dòng. Khi thử code, nên query một khoảng thời gian nhỏ trước hoặc đọc theo `chunksize`.

## 6. Bắt Đầu Làm Model

Flow ngắn gọn:

```text
Connect database
-> đọc ml.gold_demand_features
-> tạo feature time/lag/rolling
-> chia train/validation/test theo thời gian
-> train và đánh giá model
```

Không chia train/test ngẫu nhiên cho bài toán time series.

Khi lưu kết quả model, ghi lại `gold_processed_timestamp` để biết model được train trên snapshot nào.

## Không Làm Những Việc Này

- Không đọc hoặc chia sẻ file `.env`.
- Không dùng tài khoản owner database.
- Không chạy `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`.
- Không tự refresh pipeline khi chưa thống nhất với người quản lý dự án.
