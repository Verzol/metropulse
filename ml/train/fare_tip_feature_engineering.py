"""
Feature Engineering cho Fare & Tip Estimation
Nhận dữ liệu từ ml.gold_fare_tip_features, clean và tạo derived features.
"""
import pandas as pd
import numpy as np


def build_fare_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nhận df từ load_fare_tip_features().
    Trả về df đã clean + derived features, sẵn sàng train.

    Cleaning:
      - Fill null: passenger_count → 1, ratecode_id → 1
      - Loại outliers: fare > $500, distance > 150 miles

    Derived:
      - is_weekend   : day_of_week IN (5,6) — pandas: 0=Mon, 6=Sun
      - is_rush_hour : hour IN (7,8,9,17,18,19)
      - is_raining   : precipitation_mm > 0
      - is_cold      : temperature_f < 36
    """
    df = df.copy()

    # ── Fill null ──────────────────────────────────────────
    df["passenger_count"] = df["passenger_count"].fillna(1.0)
    df["ratecode_id"]     = df["ratecode_id"].fillna(1.0)

    # ── Loại outliers ──────────────────────────────────────
    before = len(df)
    df = df[
        (df["fare_amount"]   >   0.0) &
        (df["fare_amount"]   < 500.0) &
        (df["trip_distance"] >   0.0) &
        (df["trip_distance"] < 150.0)
    ].copy()
    after = len(df)
    print(f"[fare_fe] Dropped {before - after:,} outlier rows | "
          f"Remaining: {after:,}")

    # ── Derived time features ──────────────────────────────
    # day_of_week: pandas convention 0=Monday, 6=Sunday
    df["is_weekend"]   = df["day_of_week"].isin([5, 6]).astype("int8")
    df["is_rush_hour"] = df["hour"].isin([7, 8, 9, 17, 18, 19]).astype("int8")

    # ── Derived weather features ───────────────────────────
    df["is_raining"] = (df["precipitation_mm"] > 0.0).astype("int8")
    df["is_cold"]    = (df["temperature_f"]    < 36.0).astype("int8")

    return df.reset_index(drop=True)


def build_tip_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nhận df đã qua build_fare_features().
    Filter payment_type=1 (credit card) và loại tip_percent bất thường.

    Lý do bắt buộc filter credit card:
      TLC chỉ tự động ghi nhận tip credit card.
      Cash tip luôn = 0.0 trong database dù thực tế khách có tip.
      Train cả cash → model học "tip thường = 0" → sai với credit card trips.
    """
    df = df.copy()

    # Filter credit card
    before = len(df)
    df = df[df["payment_type"] == 1].copy()
    after  = len(df)
    print(f"[tip_fe] Filter payment_type=1: "
          f"{before:,} → {after:,} rows "
          f"(loại {before - after:,} cash trips)")

    # Loại tip_percent bất thường
    before2 = len(df)
    df = df[
        (df["tip_percent"] >= 0.0) &
        (df["tip_percent"] <= 100.0)
    ].copy()
    print(f"[tip_fe] Filter tip_percent [0-100%]: "
          f"{before2:,} → {len(df):,} rows")

    return df.reset_index(drop=True)


def get_feature_columns(cfg: dict) -> list:
    """Trả về list features đầy đủ (base + derived) từ config."""
    return cfg["features"]["base"] + cfg["features"]["derived"]


def get_fare_target(cfg: dict) -> str:
    return cfg["features"]["fare_target"]


def get_tip_target(cfg: dict) -> str:
    return cfg["features"]["tip_target"]