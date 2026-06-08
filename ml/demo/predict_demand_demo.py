"""
Demo Dự đoán Demand — Tháng 12/2024 (Bản hoàn thiện đồng bộ tối ưu)
Load Nov+Dec để tính lag đúng, chỉ predict tháng 12.

Chạy:
    cd ml/
    python demo/predict_demand_demo.py
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np
import pandas as pd
import xgboost as xgb
from sqlalchemy import text
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

from train.feature_engineering import AIRPORT_ZONES, MANHATTAN_CORE_ZONES

MODEL_PATH  = "models/demand_xgb.json"
OUTPUT_PATH = "demo/data_demo/demand_december_predictions.csv"

# Danh sách 24 Features đồng bộ hoàn toàn với cấu hình huấn luyện mới
FEATURES = [
    'pu_location_id', 'hour', 'day_of_week', 'month', 'temperature_f', 'precipitation_mm', 
    'is_weekend', 'is_rush_hour', 'is_transient_weekend_night', 'is_cold', 'is_raining', 'is_holiday', 
    'demand_lag1h', 'demand_lag2h', 'demand_lag24h', 'demand_lag48h', 'demand_lag168h', 'demand_vmo_3day', 
    'is_airport', 'is_manhattan_core', 'airport_rush_hour', 'manhattan_weekend', 'airport_cold', 'manhattan_rain'
]

ZONE_NAMES = {
    132: "JFK Airport",       138: "LaGuardia Airport",
    1  : "Newark Airport",    161: "Midtown Center",
    162: "Midtown East",      163: "Midtown North",
    186: "Penn Station",      230: "Times Sq/Theatre District",
    237: "Upper East Side S", 236: "Upper East Side N",
    170: "Murray Hill",       234: "Union Square",
    48 : "Clinton East",      87 : "Financial District N",
    88 : "Financial District S",
}


def load_model():
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"Model không tìm thấy: '{MODEL_PATH}'\n"
            f"Chạy trước: python train/demand_model.py"
        )
    model = xgb.XGBRegressor()
    model.load_model(MODEL_PATH)
    print(f"✅ Model loaded — best_iter: {model.best_iteration}")
    return model


def load_nov_dec_data() -> pd.DataFrame:
    """
    Load từ 01/11 để tính đủ lag168h (7 ngày) cho đầu tháng 12.
    """
    from db import get_engine
    engine = get_engine()
    query = text("""
        SELECT
            to_char(pickup_hour, 'YYYY-MM-DD HH24:MI:SS') AS pickup_hour,
            pu_location_id,
            demand,
            hour,
            day_of_week,
            month,
            temperature_f,
            precipitation_mm
        FROM ml.gold_demand_features_utc_fix
        WHERE pickup_hour >= '2024-11-01'
          AND pickup_hour <  '2025-01-01'
        ORDER BY pickup_hour, pu_location_id
    """)
    df = pd.read_sql(query, engine)
    df["pickup_hour"] = pd.to_datetime(df["pickup_hour"])
    print(f"✅ Loaded {len(df):,} rows (Nov+Dec) | "
          f"{df['pickup_hour'].min()} → {df['pickup_hour'].max()}")
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["pu_location_id", "pickup_hour"]).reset_index(drop=True)

    # ĐỒNG BỘ: Ép kiểu sang Category giống hệt lúc Train để kích hoạt giải thuật chia cây tối ưu
    df["pu_location_id"] = df["pu_location_id"].astype("category")
    df["hour"]           = df["hour"].astype("category")
    df["day_of_week"]    = df["day_of_week"].astype("category")

    # Ép kiểu số tạm thời để tính toán các toán tử logic boolean
    dow_int = df["day_of_week"].astype(int)
    hour_int = df["hour"].astype(int)

    # Time Features
    df["is_weekend"]   = dow_int.isin([6, 7]).astype("int8")
    df["is_rush_hour"] = hour_int.isin([7, 8, 9, 17, 18, 19]).astype("int8")
    
    # Feature "Vàng" đêm cuối tuần xử lý triệt để hiện tượng lệch >100 chuyến rạng sáng
    df["is_transient_weekend_night"] = (dow_int.isin([6, 7]) & hour_int.isin([0, 1, 2, 3, 4])).astype("int8")

    # Weather Features
    df["is_cold"]    = (df["temperature_f"]   < 36).astype("int8")
    df["is_raining"] = (df["precipitation_mm"] > 0).astype("int8")

    # ĐỒNG BỘ: Tính toán hệ thống Lag mở rộng bao phủ chu kỳ (trên toàn bộ Nov+Dec)
    grp = df.groupby("pu_location_id", observed=False)["demand"]
    df["demand_lag1h"]   = grp.shift(1)
    df["demand_lag2h"]   = grp.shift(2)
    df["demand_lag24h"]  = grp.shift(24)
    df["demand_lag48h"]  = grp.shift(48)
    df["demand_lag168h"] = grp.shift(168)
    
    # Trung bình trượt nhu cầu cùng khung giờ của 3 ngày gần nhất nhằm giảm nhiễu cục bộ
    df["demand_vmo_3day"] = (grp.shift(24) + grp.shift(48) + grp.shift(72)) / 3

    # Zone Features
    loc_id_int = df["pu_location_id"].astype(int)
    df["is_airport"]        = loc_id_int.isin(AIRPORT_ZONES.values()).astype("int8")
    df["is_manhattan_core"] = loc_id_int.isin(MANHATTAN_CORE_ZONES).astype("int8")

    # Interaction Features
    df["airport_rush_hour"] = (df["is_airport"]        & df["is_rush_hour"]).astype("int8")
    df["manhattan_weekend"] = (df["is_manhattan_core"] & df["is_weekend"]).astype("int8")
    df["airport_cold"]      = (df["is_airport"]        & df["is_cold"]).astype("int8")
    df["manhattan_rain"]    = (df["is_manhattan_core"] & df["is_raining"]).astype("int8")

    # Holiday — Nov+Dec 2024
    HOLIDAYS = {"2024-11-11", "2024-11-28", "2024-12-25"}
    df["date"]       = df["pickup_hour"].dt.date.astype(str)
    df["is_holiday"] = df["date"].isin(HOLIDAYS).astype("int8")
    df.drop(columns=["date"], inplace=True)

    # Bóc tách cô lập: Chỉ giữ lại dữ liệu thuộc phạm vi Tháng 12 để chạy dự đoán kiểm định
    df = df[df["pickup_hour"] >= "2024-12-01"].copy()

    # ĐỒNG BỘ: Drop bỏ toàn bộ các dòng đầu tháng bị NaN do thiếu lịch sử dịch chuyển (Lag mới)
    lag_cols = ["demand_lag1h", "demand_lag2h", "demand_lag24h", "demand_lag48h", "demand_lag168h", "demand_vmo_3day"]
    before = len(df)
    df = df.dropna(subset=lag_cols).reset_index(drop=True)
    print(f"[lag] Dropped {before - len(df)} NaN rows | Remaining: {len(df):,}")
    
    return df


def predict(model, df):
    y_pred = np.maximum(model.predict(df[FEATURES]), 0.0)
    df = df.copy()
    df["predicted_demand"] = np.round(y_pred, 1)
    df["error"]            = df["predicted_demand"] - df["demand"]
    df["abs_error"]        = np.abs(df["error"])
    df["pct_error"]        = np.where(
        df["demand"] > 0,
        np.abs(df["error"] / df["demand"]) * 100,
        np.nan,
    )
    df["zone_name"] = df["pu_location_id"].map(
        lambda x: ZONE_NAMES.get(int(x), f"Zone {x}")
    )
    return df


def display_results(df):
    valid = df.dropna(subset=["pct_error"])
    rmse  = float(np.sqrt(mean_squared_error(valid["demand"], valid["predicted_demand"])))
    mae   = float(mean_absolute_error(valid["demand"], valid["predicted_demand"]))
    r2    = float(r2_score(valid["demand"], valid["predicted_demand"]))
    mape  = float(valid["pct_error"].mean())

    print("\n" + "=" * 60)
    print("DEMO DỰ ĐOÁN DEMAND — THÁNG 12/2024 (CHƯA TỪNG THẤY)")
    print("=" * 60)
    print(f"  Tổng rows : {len(df):,} | Zones: {df['pu_location_id'].nunique()}")
    print(f"  Period    : {df['pickup_hour'].min()} → {df['pickup_hour'].max()}")
    print(f"\n  RMSE      : {rmse:.4f} chuyến/giờ/zone")
    print(f"  MAE       : {mae:.4f} chuyến/giờ/zone")
    print(f"  MAPE      : {mape:.2f}%")
    print(f"  R²        : {r2:.4f}")
    print(f"\n  So paper  : RMSE {rmse:.2f} vs 38.51 "
          f"({'✅ tốt hơn' if rmse < 38.51 else '⚠️ kém hơn'})")

    # Mẫu dự đoán — top zones
    print("\n" + "-" * 60)
    print("MẪU DỰ ĐOÁN — TOP ZONES (lag đã đúng từ tháng 11)")
    print("-" * 60)
    top_zones = (df.groupby("pu_location_id", observed=False)["demand"]
                   .mean().nlargest(8).index.tolist())
    sample = (df[df["pu_location_id"].isin(top_zones)]
                .sort_values(["pickup_hour", "pu_location_id"])
                .head(40)
              [["pickup_hour", "zone_name", "demand",
                "predicted_demand", "abs_error", "pct_error",
                "temperature_f", "precipitation_mm"]])
    sample = sample.rename(columns={
        "pickup_hour"      : "Giờ",
        "zone_name"        : "Khu vực",
        "demand"           : "Thực tế",
        "predicted_demand" : "Dự đoán",
        "abs_error"        : "Sai số",
        "pct_error"        : "Sai số %",
        "temperature_f"    : "Nhiệt(°F)",
        "precipitation_mm" : "Mưa(mm)",
    })
    sample["Sai số %"] = sample["Sai số %"].round(1).astype(str) + "%"
    sample["Giờ"] = sample["Giờ"].dt.strftime("%m-%d %H:%M")
    print(sample.to_string(index=False))

    # Theo giờ (Đồng bộ observed=False tránh Warning)
    print("\n" + "-" * 60)
    print("SAI SỐ THEO KHUNG GIỜ")
    print("-" * 60)
    hourly = (df.groupby("hour", observed=False)
                .agg(demand_mean=("demand","mean"),
                     pred_mean=("predicted_demand","mean"),
                     mae=("abs_error","mean"),
                     mape=("pct_error","mean"))
                .round(2))
    hourly.columns = ["TB Thực tế", "TB Dự đoán", "MAE", "MAPE%"]
    hourly.index.name = "Giờ"
    print(hourly.to_string())

    # Theo zone type (Đồng bộ observed=False tránh Warning)
    print("\n" + "-" * 60)
    print("SAI SỐ THEO LOẠI ZONE")
    print("-" * 60)
    df["zone_type"] = "Bình thường"
    df.loc[df["is_airport"]        == 1, "zone_type"] = "Sân bay"
    df.loc[df["is_manhattan_core"] == 1, "zone_type"] = "Manhattan core"
    zt = (df.groupby("zone_type", observed=False)
            .agg(rows=("demand","count"),
                 demand_avg=("demand","mean"),
                 pred_avg=("predicted_demand","mean"),
                 mae=("abs_error","mean"),
                 mape=("pct_error","mean"))
            .round(2))
    zt.columns = ["Rows", "TB Thực tế", "TB Dự đoán", "MAE", "MAPE%"]
    print(zt.to_string())

    # Christmas
    print("\n" + "-" * 60)
    print("CHRISTMAS (25/12) VS NGÀY THƯỜNG")
    print("-" * 60)
    df["is_xmas"] = df["pickup_hour"].dt.date.astype(str).str.startswith("2024-12-25")
    xmas = df.groupby("is_xmas", observed=False).agg(
        demand_avg=("demand","mean"),
        pred_avg=("predicted_demand","mean"),
        mae=("abs_error","mean"),
    ).round(2)
    xmas.index = ["Ngày thường", "Christmas 25/12"]
    xmas.columns = ["TB Thực tế", "TB Dự đoán", "MAE"]
    print(xmas.to_string())

    return {"rmse": rmse, "mae": mae, "mape": mape, "r2": r2}


# ─────────────────────────────────────────────────────────────────────────────
# PHẦN BỔ SUNG: Biểu đồ trực quan — không thay đổi bất kỳ logic nào ở trên
# ─────────────────────────────────────────────────────────────────────────────

def plot_line_chart_jfk(df: pd.DataFrame, zone_id: int = 132,
                        save_path: str = "demo/chart_line_jfk.png"):
    """
    Biểu đồ đường: Thực tế vs Dự báo theo giờ trong ngày cho một zone điển hình.
    Mặc định zone_id=132 (JFK Airport). Tổng hợp trung bình toàn tháng 12 theo hour.
    Có thể truyền zone_id khác tuỳ ý mà không cần sửa logic.
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        print("[chart] matplotlib chưa cài — bỏ qua biểu đồ đường. Chạy: pip install matplotlib")
        return

    zone_df = df[df["pu_location_id"].astype(int) == zone_id].copy()
    if zone_df.empty:
        print(f"[chart] Không có dữ liệu cho zone_id={zone_id}, bỏ qua line chart.")
        return

    zone_name = zone_df["zone_name"].iloc[0]

    # Tổng hợp trung bình demand theo giờ trong ngày (0–23)
    hourly = (
        zone_df
        .groupby(zone_df["pickup_hour"].dt.hour, observed=False)
        .agg(actual=("demand", "mean"), predicted=("predicted_demand", "mean"))
        .reset_index()
        .rename(columns={"pickup_hour": "hour"})
    )

    fig, ax = plt.subplots(figsize=(11, 5))
    fig.patch.set_facecolor("#0f1117")
    ax.set_facecolor("#0f1117")

    # Đường thực tế & dự báo
    ax.plot(hourly["hour"], hourly["actual"],
            color="#4fc3f7", linewidth=2.2, marker="o", markersize=5,
            label="Thực tế", zorder=3)
    ax.plot(hourly["hour"], hourly["predicted"],
            color="#ff7043", linewidth=2.2, linestyle="--", marker="s", markersize=4,
            label="Dự báo", zorder=3)

    # Vùng sai lệch
    ax.fill_between(hourly["hour"], hourly["actual"], hourly["predicted"],
                    alpha=0.12, color="#ffffff")

    # Vùng rush hour (7–9h sáng, 17–19h chiều)
    for band_start, band_end in [(7, 9), (17, 19)]:
        ax.axvspan(band_start, band_end, color="#ffd54f", alpha=0.07)

    # Trục & định dạng
    ax.set_xticks(range(0, 24))
    ax.set_xticklabels([f"{h:02d}h" for h in range(24)],
                       rotation=45, fontsize=8, color="#b0bec5")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v)}"))
    ax.tick_params(axis="y", colors="#b0bec5", labelsize=9)
    for spine in ax.spines.values():
        spine.set_edgecolor("#2a2d35")
    ax.grid(axis="y", color="#2a2d35", linewidth=0.8, linestyle="--")
    ax.grid(axis="x", color="#2a2d35", linewidth=0.4, linestyle=":")

    ax.set_xlabel("Giờ trong ngày", fontsize=10, color="#90a4ae", labelpad=8)
    ax.set_ylabel("Trung bình chuyến/giờ", fontsize=10, color="#90a4ae", labelpad=8)
    ax.set_title(f"Thực tế vs Dự báo — {zone_name} (Tháng 12/2024)",
                 fontsize=13, color="#eceff1", fontweight="bold", pad=14)

    # Chú thích rush hour thủ công
    ax.text(8, ax.get_ylim()[1] * 0.97, "rush", fontsize=7,
            color="#ffd54f", ha="center", va="top", alpha=0.6)
    ax.text(18, ax.get_ylim()[1] * 0.97, "rush", fontsize=7,
            color="#ffd54f", ha="center", va="top", alpha=0.6)

    ax.legend(facecolor="#1c1f28", edgecolor="#2a2d35",
              labelcolor="#eceff1", fontsize=10, loc="upper left")

    plt.tight_layout()
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.savefig(save_path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()
    print(f"✅ Line chart saved → {save_path}")



# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PREDICT DECEMBER 2024 DEMAND")
    print("=" * 60)

    model     = load_model()
    df_raw    = load_nov_dec_data()
    df_fe     = build_features(df_raw)

    # Kiểm tra tính khớp nối thuộc tính an toàn trước khi ném vào Booster
    missing = [f for f in FEATURES if f not in df_fe.columns]
    if missing:
        raise ValueError(f"Missing features: {missing}")

    df_result = predict(model, df_fe)
    metrics   = display_results(df_result)

    os.makedirs("data_demo", exist_ok=True)
    df_result[[
        "pickup_hour", "pu_location_id", "zone_name",
        "demand", "predicted_demand",
        "error", "abs_error", "pct_error",
        "hour", "is_weekend", "is_rush_hour",
        "temperature_f", "precipitation_mm",
        "is_cold", "is_raining", "is_holiday",
        "is_airport", "is_manhattan_core",
    ]].to_csv(OUTPUT_PATH, index=False)

    print(f"\n✅ Saved → {OUTPUT_PATH}")
    print(f"   {len(df_result):,} rows | "
          f"RMSE={metrics['rmse']:.4f} | R²={metrics['r2']:.4f}")

    # ── Biểu đồ bổ sung ──────────────────────────────────────────────────────
    print("\n[chart] Đang vẽ biểu đồ...")

    # 1. Line chart — JFK Airport (zone 132) theo giờ trong ngày
    #    Đổi zone_id để vẽ zone khác, ví dụ: plot_line_chart_jfk(df_result, zone_id=161)
    plot_line_chart_jfk(df_result, zone_id=132)