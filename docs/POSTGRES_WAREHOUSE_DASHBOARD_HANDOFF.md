# Dashboard: Kết Nối FastAPI Và Streamlit Trên VM

Tài liệu này dành cho người làm Dashboard đã đăng nhập vào VM. Hướng triển
khai dự kiến là:

- `FastAPI` kết nối PostgreSQL và cung cấp API.
- `Streamlit` hiển thị dashboard, ưu tiên gọi API từ FastAPI.

Bạn không cần chạy lại pipeline và không cần mở file `.env`.

## 1. Mở Thư Mục Project

```bash
cd /home/verzol/metropulse
```

Đặt source code `FastAPI` và `Streamlit` trong thư mục làm việc đã thống nhất
với nhóm.

## 2. Nhận Thông Tin Đăng Nhập

Người quản lý project sẽ gửi riêng cho bạn:

- `DASHBOARD_DB_USER`
- `DASHBOARD_DB_PASSWORD`

Khai báo credential trong terminal chạy FastAPI:

```bash
export DASHBOARD_DB_USER='<username_duoc_cap>'
export DASHBOARD_DB_PASSWORD='<password_duoc_cap>'
```

Không commit credential vào Git và không cần đọc `.env` của project.

## 3. Thông Tin Kết Nối Database

| Thành phần | Giá trị |
|---|---|
| Host | `127.0.0.1` |
| Port | `5433` |
| Database | `metropulse_dw` |
| Schema dashboard | `mart` |

Tài khoản Dashboard chỉ có quyền đọc các bảng dashboard.

## 4. Các Bảng Để Dùng

| Bảng | Nội dung |
|---|---|
| `mart.dashboard_hourly_demand_kpi` | KPI nhu cầu theo giờ |
| `mart.dashboard_zone_summary` | Tổng hợp theo zone |
| `mart.dashboard_payment_tip_summary` | Tổng hợp thanh toán và tip |

## 5. Kết Nối PostgreSQL Từ FastAPI

Nếu môi trường Python của bạn chưa có thư viện:

```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary pandas streamlit requests
```

Kiểm tra kết nối trước khi viết endpoint:

```python
import os
import pandas as pd
from sqlalchemy import URL, create_engine

url = URL.create(
    "postgresql+psycopg2",
    username=os.environ["DASHBOARD_DB_USER"],
    password=os.environ["DASHBOARD_DB_PASSWORD"],
    host="127.0.0.1",
    port=5433,
    database="metropulse_dw",
)
engine = create_engine(url)

sample = pd.read_sql(
    "SELECT * FROM mart.dashboard_hourly_demand_kpi LIMIT 5",
    engine,
)
print(sample)
```

Nếu lệnh trên trả về dữ liệu, FastAPI đã có thể dùng kết nối này để tạo các
endpoint cho Streamlit.

## 6. Mở FastAPI Và Streamlit

Sau khi bạn đã tạo source code app, chạy trong hai terminal trên VM:

```bash
# Terminal 1: thay bằng module FastAPI của bạn
uvicorn <fastapi_module>:app --host 127.0.0.1 --port 8000 --reload

# Terminal 2: thay bằng file Streamlit của bạn
streamlit run <streamlit_app.py> --server.address 127.0.0.1 --server.port 8501
```

Nếu muốn mở giao diện trên trình duyệt máy cá nhân, từ máy cá nhân mở một
terminal SSH có port forwarding:

```bash
ssh -L 8000:127.0.0.1:8000 -L 8501:127.0.0.1:8501 <vm_user>@<vm_ip>
```

Sau đó mở:

- Streamlit: `http://localhost:8501`
- FastAPI docs: `http://localhost:8000/docs`

## Lưu Ý

- FastAPI dùng tài khoản `read-only`; không tạo, sửa hoặc xóa bảng.
- Streamlit nên gọi FastAPI thay vì mỗi trang tự kết nối trực tiếp vào DB.
- Nếu không kết nối được, gửi lại thông báo lỗi cho người quản lý VM để kiểm
  tra container PostgreSQL hoặc credential.
