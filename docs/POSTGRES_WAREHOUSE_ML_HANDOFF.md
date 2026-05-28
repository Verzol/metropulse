# ML: Kết Nối Dữ Liệu Trên VM

Tài liệu này dành cho người làm ML đã đăng nhập vào VM. Bạn chỉ cần kết nối
tới PostgreSQL bằng tài khoản read-only được cấp, không cần chạy lại pipeline
và không cần mở file `.env`.

## 1. Mở Thư Mục Project

```bash
cd /home/verzol/metropulse
```

Bạn có thể đặt notebook hoặc source code ML trong thư mục làm việc đã thống
nhất với nhóm.

## 2. Nhận Thông Tin Đăng Nhập

Người quản lý project sẽ gửi riêng cho bạn:

- `ML_DB_USER`
- `ML_DB_PASSWORD`

Khai báo credential trong terminal đang làm việc:

```bash
export ML_DB_USER='<username_duoc_cap>'
export ML_DB_PASSWORD='<password_duoc_cap>'
```

Không commit credential vào Git và không cần đọc `.env` của project.

## 3. Thông Tin Kết Nối Database

| Thành phần | Giá trị |
|---|---|
| Host | `127.0.0.1` |
| Port | `5433` |
| Database | `metropulse_dw` |
| Bảng demand forecasting | `ml.gold_demand_features` |
| Bảng fare/tip modeling | `ml.gold_fare_tip_features` |

Tài khoản ML chỉ có quyền đọc hai bảng phục vụ ML.

## 4. Kết Nối Bằng Python

Nếu môi trường Python của bạn chưa có driver:

```bash
pip install pandas sqlalchemy psycopg2-binary
```

Kiểm tra kết nối:

```python
import os
import pandas as pd
from sqlalchemy import URL, create_engine

url = URL.create(
    "postgresql+psycopg2",
    username=os.environ["ML_DB_USER"],
    password=os.environ["ML_DB_PASSWORD"],
    host="127.0.0.1",
    port=5433,
    database="metropulse_dw",
)
engine = create_engine(url)

sample = pd.read_sql(
    "SELECT * FROM ml.gold_demand_features LIMIT 5",
    engine,
)
print(sample)
```

Nếu lệnh trên trả về dữ liệu, bạn đã sẵn sàng bắt đầu notebook hoặc training
pipeline của mình.

## 5. Dữ Liệu Cần Đọc

Demand forecasting đọc bảng hourly zone-level:

```sql
SELECT *
FROM ml.gold_demand_features;
```

Fare/tip modeling đọc bảng trip-level:

```sql
SELECT *
FROM ml.gold_fare_tip_features
WHERE pickup_year_month = '2024-01';
```

`ml.gold_demand_features` có một dòng theo zone và giờ. `ml.gold_fare_tip_features`
có một dòng theo trip hợp lệ, lớn hơn nhiều; nên thử theo tháng hoặc đọc theo
`chunksize` thay vì nạp toàn bộ vào Pandas:

```python
query = """
SELECT *
FROM ml.gold_fare_tip_features
WHERE pickup_year_month >= '2024-01'
  AND pickup_year_month < '2024-04'
"""

for chunk in pd.read_sql(query, engine, chunksize=100_000):
    # Feature processing or incremental training step.
    print(chunk.shape)
```

## Lưu Ý

- Tài khoản này là `read-only`: không tạo, sửa hoặc xóa bảng.
- Với bài toán tip, cân nhắc lọc `payment_type = 1` vì tip thẻ phản ánh dữ
  liệu tip quan sát được nhất.
- Không query schema nội bộ/phục vụ publish nếu không được cấp quyền.
- Nếu không kết nối được, gửi lại thông báo lỗi cho người quản lý VM để kiểm
  tra container PostgreSQL hoặc credential.
