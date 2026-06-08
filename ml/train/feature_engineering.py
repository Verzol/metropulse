"""
Feature Engineering cho Demand Prediction
"""
import pandas as pd
import numpy as np

# ======================== CẤU HÌNH VÙNG TRỌNG ĐIỂM ========================
AIRPORT_ZONES = {
    "JFK": 132,
    "LGA": 138,
    "EWR": 1,
}

MANHATTAN_CORE_ZONES = [
    50, 68, 90, 100, 107, 113, 114, 142, 148, 158, 161, 162, 163, 164,
    166, 167, 168, 170, 186, 187, 188, 209, 211, 224, 230, 231, 232, 233,
    234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247,
    248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261,
    262, 263,
]

# ======================== NGÀY LỄ 2023 + 2024 ==============================
# Data chạy từ 2023-2024 — cần đủ cả 2 năm để is_holiday không bị = 0
# cho toàn bộ năm 2023
HOLIDAYS_2023 = [
    "2023-01-02",   # New Year's Day (observed)
    "2023-01-16",   # Martin Luther King Jr. Day
    "2023-02-20",   # Presidents' Day
    "2023-05-29",   # Memorial Day
    "2023-06-19",   # Juneteenth
    "2023-07-04",   # Independence Day
    "2023-09-04",   # Labor Day
    "2023-10-09",   # Columbus Day
    "2023-11-11",   # Veterans Day
    "2023-11-23",   # Thanksgiving Day
    "2023-12-25",   # Christmas Day
]

HOLIDAYS_2024 = [
    "2024-01-01",   # New Year's Day
    "2024-01-15",   # Martin Luther King Jr. Day
    "2024-02-19",   # Presidents' Day
    "2024-05-27",   # Memorial Day
    "2024-06-19",   # Juneteenth
    "2024-07-04",   # Independence Day
    "2024-09-02",   # Labor Day
    "2024-10-14",   # Columbus Day
    "2024-11-11",   # Veterans Day
    "2024-11-28",   # Thanksgiving Day
    "2024-12-25",   # Christmas Day
]

def build_demand_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nhận df từ gold_demand_features, trả về df đã tối ưu các features.
    """
    df = df.copy()

    df = df.sort_values(["pu_location_id", "pickup_hour"]).reset_index(drop=True)

    # ── [ÉP KIỂU CATEGORY] Giúp XGBoost tối ưu cây tốt hơn ──
    df["pu_location_id"] = df["pu_location_id"].astype("category")
    df["hour"]           = df["hour"].astype("category")
    df["day_of_week"]    = df["day_of_week"].astype("category")

    dow_int = df["day_of_week"].astype(int)
    hour_int = df["hour"].astype(int)

    df["is_weekend"]   = dow_int.isin([6, 7]).astype("int8")
    df["is_rush_hour"] = hour_int.isin([7, 8, 9, 17, 18, 19]).astype("int8")
    
    # Feature "vàng" giải quyết ca lệch >100 chuyến đêm cuối tuần (thứ 6, thứ 7 rạng sáng)
    df["is_transient_weekend_night"] = (dow_int.isin([6, 7]) & hour_int.isin([0, 1, 2, 3, 4])).astype("int8")

    # ── Weather features ───────────────────────────────────
    df["is_cold"]    = (df["temperature_f"]   < 36).astype("int8")
    df["is_raining"] = (df["precipitation_mm"] > 0).astype("int8")

    grp = df.groupby("pu_location_id")["demand"]
    df["demand_lag1h"]   = grp.shift(1)
    df["demand_lag2h"]   = grp.shift(2)   # Bắt kịp xu hướng ngắn hạn vừa diễn ra
    df["demand_lag24h"]  = grp.shift(24)
    df["demand_lag48h"]  = grp.shift(48)  # Cùng giờ ngày hôm kia
    df["demand_lag168h"] = grp.shift(168)
    
    # Trung bình trượt nhu cầu cùng khung giờ của 3 ngày gần nhất nhằm triệt tiêu nhiễu cục bộ
    df["demand_vmo_3day"] = (grp.shift(24) + grp.shift(48) + grp.shift(72)) / 3

    # ── Zone features ──────────────────────────────────────
    # Vì pu_location_id đã thành category, ta chuyển sang int để so sánh set id
    loc_id_int = df["pu_location_id"].astype(int)
    df["is_airport"]        = loc_id_int.isin(AIRPORT_ZONES.values()).astype("int8")
    df["is_manhattan_core"] = loc_id_int.isin(MANHATTAN_CORE_ZONES).astype("int8")

    # ── Interaction features ───────────────────────────────
    df["airport_rush_hour"] = (df["is_airport"]        & df["is_rush_hour"]).astype("int8")
    df["manhattan_weekend"] = (df["is_manhattan_core"] & df["is_weekend"]).astype("int8")
    df["airport_cold"]      = (df["is_airport"]        & df["is_cold"]).astype("int8")
    df["manhattan_rain"]    = (df["is_manhattan_core"] & df["is_raining"]).astype("int8")

    # ── Holiday feature ────────────────────────────────────
    holiday_set = set(HOLIDAYS_2023 + HOLIDAYS_2024)
    df["date"]       = df["pickup_hour"].dt.date.astype(str)
    df["is_holiday"] = df["date"].isin(holiday_set).astype("int8")
    df.drop(columns=["date"], inplace=True)

    # ── Drop NaN từ danh sách lag mới cập nhật ─────────────
    lag_cols = ["demand_lag1h", "demand_lag2h", "demand_lag24h", "demand_lag48h", "demand_lag168h", "demand_vmo_3day"]
    before = len(df)
    df = df.dropna(subset=lag_cols).reset_index(drop=True)
    after  = len(df)
    print(f"[feature_engineering] Dropped {before - after} rows do NaN lag features")
    print(f"[feature_engineering] Remaining: {after} rows")

    return df

def get_feature_columns(cfg: dict) -> list:
    """Trả về danh sách feature columns từ config (base + derived)."""
    return cfg["features"]["base"] + cfg["features"]["derived"]


def get_target_column(cfg: dict) -> str:
    return cfg["features"]["target"]