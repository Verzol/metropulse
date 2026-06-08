"""
Train XGBoost — Demand Prediction
Bài báo căn cứ: Correa & Moyano (2023)
Chạy:
    cd ml/
    python train/demand_model.py
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np
import pandas as pd
import yaml
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

from db import get_engine, load_demand_features
from train.feature_engineering import build_demand_features, get_feature_columns, get_target_column


def load_config(path: str = "configs/xgb_demand.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def time_based_split(df: pd.DataFrame, test_size: float = 0.2):
    """
    Chỉ tính toán trên dữ liệu từ tháng 1 đến tháng 11/2024.
    Chia 80% train nội bộ và 20% test nội bộ trên tập 11 tháng này.
    """
    # Lọc lấy dữ liệu của 11 tháng đầu năm (bỏ hoàn toàn tháng 12)
    train_val_df = df[df['pickup_hour'] < '2024-12-01'].copy()
    
    n = len(train_val_df)
    split_idx = int(n * (1 - test_size))
    train_df = train_val_df.iloc[:split_idx].copy()
    test_df  = train_val_df.iloc[split_idx:].copy()
    
    print(f"[split] Phân chia dữ liệu huấn luyện (Tháng 1 - Tháng 11):")
    print(f"   👉 Train nội bộ: {len(train_df):,} rows | {train_df['pickup_hour'].min()} → {train_df['pickup_hour'].max()}")
    print(f"   👉 Test nội bộ : {len(test_df):,} rows | {test_df['pickup_hour'].min()} → {test_df['pickup_hour'].max()}")
    return train_df, test_df


def cross_validate(X, y, params, n_splits, gap, early_stopping_rounds):
    tscv = TimeSeriesSplit(n_splits=n_splits, gap=gap)
    rmse_scores = []
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = xgb.XGBRegressor(**params, early_stopping_rounds=early_stopping_rounds)
        model.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], verbose=False)

        y_pred = model.predict(X_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        rmse_scores.append(rmse)
        print(f"  Fold {fold+1}/{n_splits} — RMSE: {rmse:.4f} | Best iter: {model.best_iteration}")
    return rmse_scores


def train_final_model(X_train, y_train, X_val, y_val, params, early_stopping_rounds, verbose):
    model = xgb.XGBRegressor(**params, early_stopping_rounds=early_stopping_rounds)
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=verbose)
    print(f"\n[train] Best iteration: {model.best_iteration}")
    return model


def evaluate(model, X_test, y_test) -> dict:
    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae  = mean_absolute_error(y_test, y_pred)
    r2   = r2_score(y_test, y_pred)
    mask = y_test > 0
    mape = np.mean(np.abs((y_test[mask] - y_pred[mask]) / y_test[mask]))

    metrics = {
        "rmse": round(rmse, 4),
        "mae":  round(mae, 4),
        "mape": round(mape, 4),
        "r2":   round(r2, 4),
        "paper_rmse": 38.51,
        "paper_r2":   0.97,
    }

    print("\n" + "="*50)
    print("KẾT QUẢ ĐÁNH GIÁ TRÊN TEST SET (NỘI BỘ THÁNG 11)")
    print("="*50)
    print(f"RMSE : {rmse:.4f}  (Paper benchmark: 38.51)")
    print(f"MAE  : {mae:.4f}")
    print(f"MAPE : {mape:.4f}")
    print(f"R²   : {r2:.4f}   (Paper benchmark: 0.97)")
    print("="*50)
    return metrics


def main():
    print("=" * 50)
    print("DEMAND PREDICTION — XGBoost Training (December Isolated)")
    print("=" * 50)

    cfg = load_config("configs/xgb_demand.yaml")
    model_params  = cfg["model"]
    train_cfg     = cfg["training"]

    print("\n[1/6] Loading data từ PostgreSQL...")
    engine = get_engine()
    df_raw = load_demand_features(engine)
    
    print("\n[2/6] Feature engineering...")
    df = build_demand_features(df_raw)

    FEATURES = get_feature_columns(cfg)
    TARGET   = get_target_column(cfg)

    # Đảm bảo sắp xếp dòng thời gian chuẩn xác trước khi cắt mốc
    df = df.sort_values("pickup_hour").reset_index(drop=True)

    # BƯỚC: TRÍCH XUẤT VÀ CẤT RIÊNG THÁNG 12 LÀM FILE DEMO
    print("\n[🚀] Đang bóc tách riêng dữ liệu tháng 12/2024 phục vụ Demo...")
    demo_df = df[df['pickup_hour'] >= '2024-12-01'].copy()
    
    os.makedirs("data_demo", exist_ok=True)
    demo_file_path = "data_demo/demand_december_demo.csv"
    demo_df.to_csv(demo_file_path, index=False)
    print(f"✅ Đã lưu {len(demo_df):,} dòng tháng 12 vào file: {demo_file_path}")

    # [3/6] Splitting data (chỉ lấy dữ liệu đến tháng 11)
    print("\n[3/6] Splitting data theo thời gian...")
    train_df, test_df = time_based_split(df, test_size=train_cfg["test_size"])

    X_train = train_df[FEATURES]
    y_train = train_df[TARGET]
    X_test  = test_df[FEATURES]
    y_test  = test_df[TARGET]

    val_size  = int(len(X_train) * 0.1)
    X_val_es  = X_train.iloc[-val_size:]
    y_val_es  = y_train.iloc[-val_size:]
    X_tr_es   = X_train.iloc[:-val_size]
    y_tr_es   = y_train.iloc[:-val_size]

    print(f"\n[4/6] TimeSeriesSplit cross-validation...")
    cv_scores = cross_validate(X_train, y_train, model_params, train_cfg["n_cv_splits"], train_cfg["cv_gap"], train_cfg["early_stopping_rounds"])
    print(f"\n  CV RMSE: {np.mean(cv_scores):.4f} ± {np.std(cv_scores):.4f}")

    print(f"\n[5/6] Training final model...")
    model = train_final_model(X_tr_es, y_tr_es, X_val_es, y_val_es, model_params, train_cfg["early_stopping_rounds"], train_cfg["verbose"])

    print("\n[6/6] Evaluating on test set...")
    metrics = evaluate(model, X_test, y_test)
    metrics["cv_rmse_mean"] = round(float(np.mean(cv_scores)), 4)
    metrics["cv_rmse_std"]  = round(float(np.std(cv_scores)), 4)
    metrics["best_iteration"] = model.best_iteration
    metrics["n_features"]   = len(FEATURES)
    metrics["features"]     = FEATURES

    os.makedirs("models", exist_ok=True)
    model_path = cfg["paths"]["model_output"]
    model.save_model(model_path)
    print(f"\n✅ Model saved → {model_path}")

    os.makedirs("logs", exist_ok=True)
    metrics_path = cfg["paths"]["metrics_output"]
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"✅ Metrics saved → {metrics_path}")

    return model, metrics


if __name__ == "__main__":
    main()