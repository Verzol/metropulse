# ML — Demand Prediction

Dự báo nhu cầu taxi theo zone × giờ dùng XGBoost.

**Bài báo căn cứ:**
Correa & Moyano (2023) — *"Analysis and prediction of New York City taxi and Uber demands"*
Journal of Applied Research and Technology, Vol.21 No.5
DOI: 10.22201/icat.24486736e.2023.21.5.2074

---

## Cấu trúc thư mục

```
ml/
├── db.py                        # Kết nối PostgreSQL, load data
├── requirements.txt
├── README.md
├── configs/
│   └── xgb_demand.yaml          # Hyperparameters + feature list
├── train/
│   ├── feature_engineering.py   # Tạo lag features, derived features
│   └── demand_model.py          # Script train chính
├── models/
│   └── demand_xgb.json          # Model sau khi train (tự sinh ra)
└── logs/
    └── demand_metrics.json      # Metrics sau khi train (tự sinh ra)
```

---

## Cách chạy

### Bước 1 — Cài đặt môi trường (chỉ làm 1 lần)

```bash
cd ml/
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

### Bước 2 — Set biến môi trường PostgreSQL

```bash
export ML_DB_USER="your_username"
export ML_DB_PASSWORD="your_password"
```

Hoặc tạo file `.env` (không commit lên git):

```bash
# .env
ML_DB_USER=your_username
ML_DB_PASSWORD=your_password
```

Sau đó load:

```bash
export $(cat .env | xargs)
```

### Bước 3 — Test kết nối database

```bash
python - <<'EOF'
from db import get_engine, load_demand_features
engine = get_engine()
df = load_demand_features(engine, limit=5)
print(df)
print(df.dtypes)
EOF
```

### Bước 4 — Chạy training

```bash
python train/demand_model.py
```

Output mẫu:

```
==================================================
DEMAND PREDICTION — XGBoost Training
Căn cứ: Correa & Moyano (2023)
==================================================

[1/6] Loading data từ PostgreSQL...
      Loaded: 50000 rows, 8 columns

[2/6] Feature engineering...
      Dropped 263 rows do NaN lag features
      Remaining: 49737 rows

[3/6] Splitting data theo thời gian (80/20)...
      Train: 39789 rows | Test: 9948 rows
      Train period: 2024-01-01 → 2024-09-30
      Test  period: 2024-10-01 → 2024-12-31

[4/6] TimeSeriesSplit cross-validation (n_splits=5, gap=24)...
  Fold 1/5 — RMSE: 42.31 | Best iter: 234
  Fold 2/5 — RMSE: 39.87 | Best iter: 312
  ...

  CV RMSE: 40.12 ± 2.34

[5/6] Training final model...
[100] val-rmse: 45.2341
[200] val-rmse: 39.8721
...

[train] Best iteration: 387

[6/6] Evaluating on test set...
==================================================
KẾT QUẢ ĐÁNH GIÁ TRÊN TEST SET
==================================================
RMSE : 38.XX  (Paper benchmark: 38.51)
MAE  : XX.XX
MAPE : 0.XXX
R²   : 0.9X   (Paper benchmark: 0.97)
==================================================

✅ Model saved → models/demand_xgb.json
✅ Metrics saved → logs/demand_metrics.json
```

---

## Giải thích features

### Features từ bài báo (Section 5.1 — Correa & Moyano 2023)

| Feature | Mô tả | Trích dẫn |
|---|---|---|
| `pu_location_id` | TAZ zone ID | "location would be a good predictor" |
| `hour` | Giờ trong ngày | "time of day is the most crucial factor" |
| `day_of_week` | Ngày trong tuần | "day of week" |
| `month` | Tháng | "seasonal variation April–September" |
| `temperature_f` | Nhiệt độ (°F) | "demand tăng khi <36°F" — Figure 2a |
| `precipitation_mm` | Lượng mưa (mm) | "hourly rainfall" — Figure 2b |

### Derived features (nhóm bổ sung)

| Feature | Công thức | Lý do |
|---|---|---|
| `is_weekend` | `day_of_week IN (6,7)` | Figure 6b — weekend pattern |
| `is_rush_hour` | `hour IN (7,8,9,17,18,19)` | Figure 6a — peak 6–8pm |
| `is_cold` | `temperature_f < 36` | Figure 2a — threshold |
| `is_raining` | `precipitation_mm > 0` | Figure 2b — threshold |
| `demand_lag1h` | `shift(1)` per zone | Time-series dependency |
| `demand_lag24h` | `shift(24)` per zone | Daily cycle |
| `demand_lag168h` | `shift(168)` per zone | Weekly cycle |

---

## Điều chỉnh hyperparameters

Mở `configs/xgb_demand.yaml` và chỉnh:

```yaml
model:
  n_estimators: 1000    # Tăng nếu early stopping chưa converge
  max_depth: 6          # Tăng 7-8 nếu underfitting
  learning_rate: 0.05   # Giảm nếu RMSE không ổn định
  colsample_bytree: 0.8 # Giảm 0.6-0.7 nếu overfitting
```

---

## Load model để predict

```python
import xgboost as xgb
import pandas as pd

model = xgb.XGBRegressor()
model.load_model("models/demand_xgb.json")

# Input mẫu: Times Square, 7pm Friday, 15°C, không mưa
X_new = pd.DataFrame([{
    "pu_location_id" : 161,
    "hour"           : 19,
    "day_of_week"    : 5,
    "month"          : 6,
    "temperature_f"  : 59.0,
    "precipitation_mm": 0.0,
    "is_weekend"     : 0,
    "is_rush_hour"   : 1,
    "is_cold"        : 0,
    "is_raining"     : 0,
    "demand_lag1h"   : 45,
    "demand_lag24h"  : 42,
    "demand_lag168h" : 40,
}])

pred = model.predict(X_new)
print(f"Predicted demand: {pred[0]:.0f} trips")
```