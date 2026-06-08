"""
Train XGBoost — Fare & Tip Estimation
Fare model : XGBoost reg:squarederror — random 80/20 split
Tip model  : XGBoost reg:absoluteerror — filter payment_type=1

Chạy:
    cd ml/
    python train/fare_tip_model.py              # Train cả 2
    python train/fare_tip_model.py --fare-only  # Chỉ train fare
    python train/fare_tip_model.py --tip-only   # Chỉ train tip
"""
import argparse
import json
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np
import pandas as pd
import yaml
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

from db import get_engine, load_fare_tip_features
from train.fare_tip_feature_engineering import (
    build_fare_features,
    build_tip_features,
    get_feature_columns,
    get_fare_target,
    get_tip_target,
)


CONFIG_PATH = "configs/xgb_fare_tip.yaml"


def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def random_split(df: pd.DataFrame, test_size: float, seed: int):
    """
    Random 80/20 split.
    Fare prediction là regression thông thường — không phải time-series
    (không có timestamp đầy đủ trong gold_fare_tip_features)
    nên random split là phù hợp.
    """
    train_df, test_df = train_test_split(
        df,
        test_size    = test_size,
        random_state = seed,
        shuffle      = True,
    )
    print(f"[split] Train: {len(train_df):,} | Test: {len(test_df):,}")
    return train_df.reset_index(drop=True), test_df.reset_index(drop=True)


def train_xgb(
    X_tr    : pd.DataFrame,
    y_tr    : pd.Series,
    X_val   : pd.DataFrame,
    y_val   : pd.Series,
    params  : dict,
    early_stopping_rounds : int,
    verbose : int,
    label   : str,
) -> xgb.XGBRegressor:

    model = xgb.XGBRegressor(
        **params,
        early_stopping_rounds=early_stopping_rounds,
    )
    model.fit(
        X_tr, y_tr,
        eval_set = [(X_val, y_val)],
        verbose  = verbose,
    )
    print(f"[{label}] Best iteration: {model.best_iteration}")
    return model


def evaluate(
    model  : xgb.XGBRegressor,
    X_test : pd.DataFrame,
    y_test : pd.Series,
    label  : str,
    unit   : str,
) -> dict:

    y_pred = np.maximum(model.predict(X_test), 0.0)

    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    mae  = float(mean_absolute_error(y_test, y_pred))
    r2   = float(r2_score(y_test, y_pred))

    mask = y_test.values > 0
    mape = float(np.mean(
        np.abs((y_test.values[mask] - y_pred[mask]) / y_test.values[mask])
    )) if mask.sum() > 0 else float("nan")

    print("\n" + "=" * 55)
    print(f"KẾT QUẢ — {label}")
    print("=" * 55)
    print(f"RMSE : {rmse:.4f} {unit}")
    print(f"MAE  : {mae:.4f} {unit}")
    print(f"MAPE : {mape:.4f}")
    print(f"R²   : {r2:.4f}")
    print("=" * 55)

    return {
        "rmse"          : round(rmse, 4),
        "mae"           : round(mae,  4),
        "mape"          : round(mape, 4),
        "r2"            : round(r2,   4),
        "best_iteration": int(model.best_iteration),
        "n_features"    : int(X_test.shape[1]),
        "features"      : list(X_test.columns),
    }


def run_fare(cfg: dict, df: pd.DataFrame):
    print("\n" + "=" * 55)
    print("FARE MODEL — XGBoost reg:squarederror")
    print("=" * 55)

    FEATURES  = get_feature_columns(cfg)
    TARGET    = get_fare_target(cfg)
    params    = cfg["fare_model"]
    train_cfg = cfg["training"]

    # Validate
    missing = [f for f in FEATURES if f not in df.columns]
    if missing:
        raise ValueError(f"Missing features: {missing}")
    if TARGET not in df.columns:
        raise ValueError(f"Target '{TARGET}' không có trong df")

    print(f"Features ({len(FEATURES)}): {FEATURES}")
    print(f"Target  : {TARGET} | Rows: {len(df):,}")

    train_df, test_df = random_split(
        df,
        test_size = train_cfg["test_size"],
        seed      = params["random_state"],
    )

    X_train = train_df[FEATURES]
    y_train = train_df[TARGET]
    X_test  = test_df[FEATURES]
    y_test  = test_df[TARGET]

    # Tách val từ train cho early stopping
    val_n = int(len(X_train) * train_cfg["val_size"])
    X_val = X_train.iloc[-val_n:]
    y_val = y_train.iloc[-val_n:]
    X_tr  = X_train.iloc[:-val_n]
    y_tr  = y_train.iloc[:-val_n]
    print(f"  Tr:{len(X_tr):,} | Val:{len(X_val):,} | Test:{len(X_test):,}")

    model = train_xgb(
        X_tr, y_tr, X_val, y_val,
        params                = params,
        early_stopping_rounds = train_cfg["early_stopping_rounds"],
        verbose               = train_cfg["verbose"],
        label                 = "FARE",
    )

    metrics = evaluate(model, X_test, y_test, label="FARE", unit="USD")

    os.makedirs("models", exist_ok=True)
    os.makedirs("logs",   exist_ok=True)
    model.save_model(cfg["paths"]["fare_model_output"])
    with open(cfg["paths"]["fare_metrics_output"], "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"✅ Model   → {cfg['paths']['fare_model_output']}")
    print(f"✅ Metrics → {cfg['paths']['fare_metrics_output']}")
    return model, metrics


def run_tip(cfg: dict, df: pd.DataFrame):
    print("\n" + "=" * 55)
    print("TIP MODEL — XGBoost reg:absoluteerror (payment_type=1 only)")
    print("=" * 55)

    FEATURES  = get_feature_columns(cfg)
    TARGET    = get_tip_target(cfg)
    params    = cfg["tip_model"]
    train_cfg = cfg["training"]

    # Filter credit card trước khi split
    df_tip = build_tip_features(df)

    missing = [f for f in FEATURES if f not in df_tip.columns]
    if missing:
        raise ValueError(f"Missing features: {missing}")
    if TARGET not in df_tip.columns:
        raise ValueError(f"Target '{TARGET}' không có trong df_tip")

    print(f"Features ({len(FEATURES)}): {FEATURES}")
    print(f"Target  : {TARGET} | Rows: {len(df_tip):,}")

    train_df, test_df = random_split(
        df_tip,
        test_size = train_cfg["test_size"],
        seed      = params["random_state"],
    )

    X_train = train_df[FEATURES]
    y_train = train_df[TARGET]
    X_test  = test_df[FEATURES]
    y_test  = test_df[TARGET]

    val_n = int(len(X_train) * train_cfg["val_size"])
    X_val = X_train.iloc[-val_n:]
    y_val = y_train.iloc[-val_n:]
    X_tr  = X_train.iloc[:-val_n]
    y_tr  = y_train.iloc[:-val_n]
    print(f"  Tr:{len(X_tr):,} | Val:{len(X_val):,} | Test:{len(X_test):,}")

    model = train_xgb(
        X_tr, y_tr, X_val, y_val,
        params                = params,
        early_stopping_rounds = train_cfg["early_stopping_rounds"],
        verbose               = train_cfg["verbose"],
        label                 = "TIP",
    )

    metrics = evaluate(model, X_test, y_test, label="TIP", unit="%")

    os.makedirs("models", exist_ok=True)
    os.makedirs("logs",   exist_ok=True)
    model.save_model(cfg["paths"]["tip_model_output"])
    with open(cfg["paths"]["tip_metrics_output"], "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"✅ Model   → {cfg['paths']['tip_model_output']}")
    print(f"✅ Metrics → {cfg['paths']['tip_metrics_output']}")
    return model, metrics


def main():
    parser = argparse.ArgumentParser(
        description="Train XGBoost Fare & Tip models"
    )
    parser.add_argument("--fare-only", action="store_true")
    parser.add_argument("--tip-only",  action="store_true")
    args = parser.parse_args()

    run_fare_flag = not args.tip_only
    run_tip_flag  = not args.fare_only

    print("=" * 55)
    print("FARE & TIP ESTIMATION — XGBoost")
    print("=" * 55)

    cfg = load_config()
    sample_pct = cfg["training"]["sample_pct"]

    # Load data
    print(f"\n[1/3] Loading ~{sample_pct}% rows từ PostgreSQL...")
    engine = get_engine()
    df_raw = load_fare_tip_features(engine, sample_pct=sample_pct)
    print(f"      Loaded: {len(df_raw):,} rows | "
          f"RAM ước tính: ~{len(df_raw) * 14 * 8 / 1e9:.2f} GB")

    # Feature engineering
    print("\n[2/3] Feature engineering...")
    df_base = build_fare_features(df_raw)

    # Train
    print("\n[3/3] Training...")
    results = {}
    if run_fare_flag:
        _, results["fare"] = run_fare(cfg, df_base)
    if run_tip_flag:
        _, results["tip"]  = run_tip(cfg, df_base)

    # Tổng kết
    print("\n" + "=" * 55)
    print("TỔNG KẾT")
    print("=" * 55)
    if "fare" in results:
        m = results["fare"]
        print(f"FARE RMSE:{m['rmse']:.4f} USD | "
              f"MAE:{m['mae']:.4f} | R²:{m['r2']:.4f} | "
              f"iter:{m['best_iteration']}")
    if "tip" in results:
        m = results["tip"]
        print(f"TIP  MAE:{m['mae']:.4f}%  | "
              f"RMSE:{m['rmse']:.4f} | R²:{m['r2']:.4f} | "
              f"iter:{m['best_iteration']}")
    print("=" * 55)


if __name__ == "__main__":
    main()