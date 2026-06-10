import os
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import SQLAlchemyError


load_dotenv()

# Tải trước mô hình XGBoost cho dự đoán Fare & Tip
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FARE_MODEL_PATH = os.path.join(PROJECT_ROOT, "ml", "models", "fare_xgb.json")
TIP_MODEL_PATH = os.path.join(PROJECT_ROOT, "ml", "models", "tip_xgb.json")

fare_model = None
tip_model = None

try:
    if os.path.exists(FARE_MODEL_PATH):
        fare_model = xgb.XGBRegressor()
        fare_model.load_model(FARE_MODEL_PATH)
        print(f"✅ Loaded Fare Model from {FARE_MODEL_PATH}")
    else:
        print(f"⚠️ Warning: Fare Model not found at {FARE_MODEL_PATH}")
except Exception as e:
    print(f"❌ Error loading Fare Model: {e}")

try:
    if os.path.exists(TIP_MODEL_PATH):
        tip_model = xgb.XGBRegressor()
        tip_model.load_model(TIP_MODEL_PATH)
        print(f"✅ Loaded Tip Model from {TIP_MODEL_PATH}")
    else:
        print(f"⚠️ Warning: Tip Model not found at {TIP_MODEL_PATH}")
except Exception as e:
    print(f"❌ Error loading Tip Model: {e}")


def fallback_predict_fare(trip_distance: float, hour: int) -> float:
    base = 3.0
    dist_charge = trip_distance * 2.5
    surcharge = 2.5 if 16 <= hour <= 20 else 0.0
    return round(base + dist_charge + surcharge, 2)


def fallback_predict_tip_pct(payment_type: int) -> float:
    return 15.0 if payment_type == 1 else 0.0



def _env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _postgres_driver() -> str:
    try:
        import psycopg2  # noqa: F401

        return "postgresql+psycopg2"
    except ImportError:
        return "postgresql+psycopg"


def build_database_url() -> URL:
    host = _env(
        "DASHBOARD_DB_HOST",
        "WAREHOUSE_POSTGRES_BIND_ADDRESS",
        default="127.0.0.1",
    )
    return URL.create(
        _postgres_driver(),
        username=_env("DASHBOARD_DB_USER", "WAREHOUSE_DASHBOARD_READER_USER"),
        password=_env("DASHBOARD_DB_PASSWORD", "WAREHOUSE_DASHBOARD_READER_PASSWORD"),
        host="127.0.0.1" if host == "localhost" else host,
        port=int(_env("DASHBOARD_DB_PORT", "WAREHOUSE_POSTGRES_HOST_PORT", default="5433")),
        database=_env("DASHBOARD_DB_NAME", "WAREHOUSE_POSTGRES_DB", default="metropulse_dw"),
    )


engine = create_engine(build_database_url(), pool_pre_ping=True, pool_size=3, max_overflow=2)

app = FastAPI(
    title="MetroPulse Dashboard API",
    version="1.0.0",
    description="Read-only API for MetroPulse dashboard marts in PostgreSQL.",
)


def fetch_all(sql: str, **params: Any) -> list[dict[str, Any]]:
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL query failed: {exc}") from exc
    return [{key: _json_value(value) for key, value in row.items()} for row in rows]


def fetch_one(sql: str, **params: Any) -> dict[str, Any]:
    rows = fetch_all(sql, **params)
    return rows[0] if rows else {}


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


@app.get("/api/health")
def health() -> dict[str, Any]:
    result = fetch_one(
        """
        SELECT
            current_database() AS database_name,
            current_user AS user_name,
            current_setting('timezone') AS timezone
        """
    )
    return {"status": "ok", **result}


@app.get("/api/meta")
def meta() -> dict[str, Any]:
    table_counts = fetch_all(
        """
        SELECT 'dashboard_hourly_demand_kpi' AS table_name, COUNT(*)::bigint AS row_count
        FROM mart.dashboard_hourly_demand_kpi
        UNION ALL
        SELECT 'dashboard_zone_summary' AS table_name, COUNT(*)::bigint AS row_count
        FROM mart.dashboard_zone_summary
        UNION ALL
        SELECT 'dashboard_payment_tip_summary' AS table_name, COUNT(*)::bigint AS row_count
        FROM mart.dashboard_payment_tip_summary
        ORDER BY table_name
        """
    )
    available_months = [
        row["pickup_year_month"]
        for row in fetch_all(
            """
            SELECT DISTINCT pickup_year_month
            FROM mart.dashboard_hourly_demand_kpi
            ORDER BY pickup_year_month
            """
        )
    ]
    month_range = fetch_one(
        """
        SELECT
            MIN(pickup_year_month) AS min_month,
            MAX(pickup_year_month) AS max_month
        FROM mart.dashboard_hourly_demand_kpi
        """
    )
    return {"tables": table_counts, "available_months": available_months, **month_range}


@app.get("/api/summary")
def summary(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
) -> dict[str, Any]:
    filters = """
        WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
          AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
    """
    hourly = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(total_demand), 0)::bigint AS total_demand,
            COUNT(*)::bigint AS hourly_points,
            COALESCE(AVG(active_zones), 0)::float AS avg_active_zones,
            COALESCE(AVG(avg_demand_per_active_zone), 0)::float AS avg_demand_per_active_zone,
            COALESCE(AVG(avg_temperature_f), 0)::float AS avg_temperature_f,
            COALESCE(AVG(avg_precipitation_mm), 0)::float AS avg_precipitation_mm
        FROM mart.dashboard_hourly_demand_kpi
        {filters}
        """,
        start_month=start_month,
        end_month=end_month,
    )
    peak_hour = fetch_one(
        f"""
        SELECT
            pickup_hour AS peak_hour,
            total_demand AS peak_total_demand
        FROM mart.dashboard_hourly_demand_kpi
        {filters}
        ORDER BY total_demand DESC, pickup_hour ASC
        LIMIT 1
        """,
        start_month=start_month,
        end_month=end_month,
    )
    payment = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(trip_count), 0)::bigint AS fare_tip_trip_count,
            COALESCE(AVG(avg_fare_amount), 0)::float AS avg_fare_amount,
            COALESCE(AVG(avg_tip_percent), 0)::float AS avg_tip_percent
        FROM mart.dashboard_payment_tip_summary
        {filters}
        """,
        start_month=start_month,
        end_month=end_month,
    )
    return {**hourly, **peak_hour, **payment}


@app.get("/api/hourly-demand")
def hourly_demand(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    limit: int = Query(default=5000, ge=1, le=20000),
) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            pickup_hour,
            pickup_year_month,
            hour,
            day_of_week,
            month,
            total_demand,
            active_zones,
            avg_demand_per_active_zone,
            avg_temperature_f,
            avg_precipitation_mm
        FROM mart.dashboard_hourly_demand_kpi
        WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
          AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
        ORDER BY pickup_hour
        LIMIT :limit
        """,
        start_month=start_month,
        end_month=end_month,
        limit=limit,
    )


@app.get("/api/zone-summary")
def zone_summary(limit: int = Query(default=25, ge=1, le=263)) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            pu_location_id,
            pickup_borough,
            pickup_zone,
            pickup_latitude,
            pickup_longitude,
            total_demand,
            avg_hourly_demand,
            max_hourly_demand,
            active_hours
        FROM mart.dashboard_zone_summary
        ORDER BY total_demand DESC
        LIMIT :limit
        """,
        limit=limit,
    )


@app.get("/api/payment-tip-summary")
def payment_tip_summary(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
) -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT
            pickup_year_month,
            payment_type,
            trip_count,
            avg_fare_amount,
            avg_tip_amount,
            avg_tip_percent,
            median_tip_percent,
            median_fare_amount,
            avg_trip_distance
        FROM mart.dashboard_payment_tip_summary
        WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
          AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
        ORDER BY pickup_year_month, payment_type
        """,
        start_month=start_month,
        end_month=end_month,
    )


@app.get("/api/zones-all")
def zones_all() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT DISTINCT
            pu_location_id AS zone_id,
            pickup_zone AS zone_name,
            pickup_borough AS borough
        FROM mart.dashboard_zone_summary
        ORDER BY pickup_zone
        """
    )


@app.get("/api/ml-metrics-all")
def ml_metrics_all() -> dict[str, Any]:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    demand_path = os.path.join(project_root, "ml", "logs", "demand_metrics.json")
    fare_path = os.path.join(project_root, "ml", "logs", "fare_metrics.json")
    tip_path = os.path.join(project_root, "ml", "logs", "tip_metrics.json")
    
    metrics = {
        "demand": {},
        "fare": {},
        "tip": {}
    }
    
    try:
        if os.path.exists(demand_path):
            with open(demand_path, "r", encoding="utf-8") as f:
                metrics["demand"] = json.load(f)
    except Exception as e:
        print(f"Error loading demand metrics: {e}")
        
    try:
        if os.path.exists(fare_path):
            with open(fare_path, "r", encoding="utf-8") as f:
                metrics["fare"] = json.load(f)
    except Exception as e:
        print(f"Error loading fare metrics: {e}")
        
    try:
        if os.path.exists(tip_path):
            with open(tip_path, "r", encoding="utf-8") as f:
                metrics["tip"] = json.load(f)
    except Exception as e:
        print(f"Error loading tip metrics: {e}")
        
    return metrics


class FareTipPredictRequest(BaseModel):
    trip_distance: float
    pu_location_id: int
    do_location_id: int
    passenger_count: int
    ratecode_id: int
    hour: int
    day_of_week: int
    month: int
    temperature_f: float
    precipitation_mm: float
    payment_type: int


@app.post("/api/predict/fare-tip")
def predict_fare_tip(req: FareTipPredictRequest) -> dict[str, Any]:
    # Tính toán các derived features
    is_rush_hour = 1 if req.hour in [7, 8, 9, 17, 18, 19] else 0
    is_weekend = 1 if req.day_of_week in [5, 6] else 0
    is_raining = 1 if req.precipitation_mm > 0.0 else 0
    is_cold = 1 if req.temperature_f < 36.0 else 0
    
    # Check if models are available
    if fare_model is not None or tip_model is not None:
        try:
            features_df = pd.DataFrame([{
                "trip_distance": float(req.trip_distance),
                "pu_location_id": int(req.pu_location_id),
                "do_location_id": int(req.do_location_id),
                "passenger_count": float(req.passenger_count),
                "ratecode_id": float(req.ratecode_id),
                "hour": int(req.hour),
                "day_of_week": int(req.day_of_week),
                "month": int(req.month),
                "temperature_f": float(req.temperature_f),
                "precipitation_mm": float(req.precipitation_mm),
                "is_rush_hour": int(is_rush_hour),
                "is_weekend": int(is_weekend),
                "is_raining": int(is_raining),
                "is_cold": int(is_cold)
            }])
            
            # Predict Fare
            if fare_model is not None:
                pred_fare = float(fare_model.predict(features_df)[0])
                pred_fare = max(0.0, round(pred_fare, 2))
            else:
                pred_fare = fallback_predict_fare(req.trip_distance, req.hour)
                
            # Predict Tip %
            if req.payment_type == 1:
                if tip_model is not None:
                    pred_tip_pct = float(tip_model.predict(features_df)[0])
                    pred_tip_pct = max(0.0, round(pred_tip_pct, 2))
                else:
                    pred_tip_pct = 15.0
            else:
                pred_tip_pct = 0.0
                
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Model inference failed: {exc}")
    else:
        # Fallback if both models are missing
        pred_fare = fallback_predict_fare(req.trip_distance, req.hour)
        pred_tip_pct = fallback_predict_tip_pct(req.payment_type)
        
    # Calculate values
    tip_amount = round(pred_fare * (pred_tip_pct / 100.0), 2)
    total_amount = round(pred_fare + tip_amount, 2)
    
    return {
        "predicted_fare": pred_fare,
        "predicted_tip_percent": pred_tip_pct,
        "predicted_tip_amount": tip_amount,
        "total_amount": total_amount,
        "model_used": (fare_model is not None and tip_model is not None),
        "derived_features": {
            "is_rush_hour": is_rush_hour,
            "is_weekend": is_weekend,
            "is_raining": is_raining,
            "is_cold": is_cold
        }
    }

