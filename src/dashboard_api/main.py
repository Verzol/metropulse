import os
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from dateutil.relativedelta import relativedelta

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.dashboard_api.prediction import (
    FARE_TIP_FEATURE_COLUMNS,
    build_prediction_features,
    calculate_zone_distance,
    load_zone_lookup,
)

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
    if isinstance(value, np.generic):
        return value.item()
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

def get_prev_period(start_month: str | None, end_month: str | None) -> tuple[str | None, str | None]:
    if not start_month or not end_month:
        return None, None
    try:
        sm = datetime.strptime(start_month, "%Y-%m")
        em = datetime.strptime(end_month, "%Y-%m")
        diff_months = (em.year - sm.year) * 12 + (em.month - sm.month) + 1
        prev_em = sm - relativedelta(months=1)
        prev_sm = prev_em - relativedelta(months=diff_months - 1)
        return prev_sm.strftime("%Y-%m"), prev_em.strftime("%Y-%m")
    except ValueError:
        return None, None



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
            COALESCE(AVG(avg_precipitation_mm), 0)::float AS avg_precipitation_mm,
            MIN(pickup_hour) AS first_data_hour,
            MAX(pickup_hour) AS last_data_hour,
            MAX(source_gold_processed_timestamp) AS latest_source_processed_at,
            MAX(dashboard_processed_timestamp) AS latest_dashboard_processed_at,
            COALESCE(SUM(total_demand) FILTER (
                WHERE EXTRACT(ISODOW FROM pickup_hour) IN (6, 7)
            ), 0)::bigint AS weekend_demand,
            COALESCE(SUM(total_demand) FILTER (
                WHERE hour IN (7, 8, 9, 17, 18, 19)
            ), 0)::bigint AS rush_hour_demand
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
            COALESCE(
                SUM(avg_fare_amount * trip_count) / NULLIF(SUM(trip_count), 0),
                0
            )::float AS avg_fare_amount,
            COALESCE(
                SUM(avg_tip_percent * trip_count) / NULLIF(SUM(trip_count), 0),
                0
            )::float AS avg_tip_percent
        FROM mart.dashboard_payment_tip_summary
        {filters}
        """,
        start_month=start_month,
        end_month=end_month,
    )
    payment_leader = fetch_one(
        f"""
        SELECT payment_type, SUM(trip_count)::bigint AS trip_count
        FROM mart.dashboard_payment_tip_summary
        {filters}
        GROUP BY payment_type
        ORDER BY trip_count DESC, payment_type
        LIMIT 1
        """,
        start_month=start_month,
        end_month=end_month,
    )
    total_demand = float(hourly.get("total_demand") or 0)
    weekend_demand = float(hourly.get("weekend_demand") or 0)
    rush_hour_demand = float(hourly.get("rush_hour_demand") or 0)
    fare_tip_trip_count = float(payment.get("fare_tip_trip_count") or 0)
    leader_trip_count = float(payment_leader.get("trip_count") or 0)
    
    prev_sm, prev_em = get_prev_period(start_month, end_month)
    prev_hourly = {}
    prev_payment = {}
    if prev_sm and prev_em:
        prev_hourly = fetch_one(
            f"""
            SELECT
                COALESCE(SUM(total_demand), 0)::bigint AS prev_total_demand,
                COALESCE(SUM(total_demand) / NULLIF(COUNT(*), 0), 0)::float AS prev_avg_hourly_demand,
                COALESCE(AVG(active_zones), 0)::float AS prev_avg_active_zones,
                COALESCE(AVG(avg_demand_per_active_zone), 0)::float AS prev_avg_demand_per_active_zone
            FROM mart.dashboard_hourly_demand_kpi
            {filters}
            """,
            start_month=prev_sm,
            end_month=prev_em,
        )
        prev_payment = fetch_one(
            f"""
            SELECT
                COALESCE(
                    SUM(avg_fare_amount * trip_count) / NULLIF(SUM(trip_count), 0),
                    0
                )::float AS prev_avg_fare_amount
            FROM mart.dashboard_payment_tip_summary
            {filters}
            """,
            start_month=prev_sm,
            end_month=prev_em,
        )

    return {
        **hourly,
        **peak_hour,
        **payment,
        **prev_hourly,
        **prev_payment,
        "weekend_share": weekend_demand / total_demand if total_demand else 0.0,
        "rush_hour_share": rush_hour_demand / total_demand if total_demand else 0.0,
        "leading_payment_type": payment_leader.get("payment_type"),
        "leading_payment_trip_count": int(leader_trip_count),
        "leading_payment_share": (
            leader_trip_count / fare_tip_trip_count if fare_tip_trip_count else 0.0
        ),
    }


@app.get("/api/demand-trends")
def demand_trends(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
) -> dict[str, list[dict[str, Any]]]:
    filters = """
        WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
          AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
    """
    params = {"start_month": start_month, "end_month": end_month}
    monthly = fetch_all(
        f"""
        SELECT
            pickup_year_month,
            SUM(total_demand)::bigint AS total_demand,
            AVG(active_zones)::float AS avg_active_zones,
            AVG(avg_temperature_f)::float AS avg_temperature_f,
            AVG(avg_precipitation_mm)::float AS avg_precipitation_mm
        FROM mart.dashboard_hourly_demand_kpi
        {filters}
        GROUP BY pickup_year_month
        ORDER BY pickup_year_month
        """,
        **params,
    )
    hourly = fetch_all(
        f"""
        SELECT
            hour,
            SUM(total_demand)::bigint AS total_demand,
            AVG(avg_temperature_f)::float AS avg_temperature_f,
            AVG(avg_precipitation_mm)::float AS avg_precipitation_mm
        FROM mart.dashboard_hourly_demand_kpi
        {filters}
        GROUP BY hour
        ORDER BY hour
        """,
        **params,
    )
    weekday = fetch_all(
        f"""
        SELECT
            EXTRACT(ISODOW FROM pickup_hour)::int AS iso_day_of_week,
            SUM(total_demand)::bigint AS total_demand,
            AVG(avg_temperature_f)::float AS avg_temperature_f
        FROM mart.dashboard_hourly_demand_kpi
        {filters}
        GROUP BY EXTRACT(ISODOW FROM pickup_hour)
        ORDER BY iso_day_of_week
        """,
        **params,
    )
    return {"monthly": monthly, "hourly": hourly, "weekday": weekday}


@app.get("/api/hourly-demand")
def hourly_demand(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    limit: int = Query(default=5000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    filters = """
        WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
          AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
    """
    total = fetch_one(
        f"""
        SELECT COUNT(*)::bigint AS total_rows
        FROM mart.dashboard_hourly_demand_kpi
        {filters}
        """,
        start_month=start_month,
        end_month=end_month,
    )
    rows = fetch_all(
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
        ORDER BY pickup_hour DESC
        LIMIT :limit
        OFFSET :offset
        """,
        start_month=start_month,
        end_month=end_month,
        limit=limit,
        offset=offset,
    )
    return {
        "rows": rows,
        "total_rows": int(total.get("total_rows") or 0),
        "limit": limit,
        "offset": offset,
    }


@app.get("/api/zone-summary")
def zone_summary(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    limit: int = Query(default=25, ge=1, le=263),
) -> list[dict[str, Any]]:
    return fetch_all(
        """
        WITH filtered AS (
            SELECT *
            FROM mart.dashboard_zone_summary
            WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
              AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
        ),
        aggregated AS (
            SELECT
                pu_location_id,
                MAX(pickup_borough) AS pickup_borough,
                MAX(pickup_zone) AS pickup_zone,
                MAX(pickup_latitude) AS pickup_latitude,
                MAX(pickup_longitude) AS pickup_longitude,
                SUM(total_demand)::bigint AS total_demand,
                SUM(active_hours)::bigint AS active_hours,
                MAX(max_hourly_demand)::int AS max_hourly_demand,
                MIN(first_pickup_hour) AS first_pickup_hour,
                MAX(last_pickup_hour) AS last_pickup_hour,
                (
                    SUM(avg_temperature_f * active_hours)
                    / NULLIF(SUM(active_hours), 0)
                )::float AS avg_temperature_f,
                (
                    SUM(avg_precipitation_mm * active_hours)
                    / NULLIF(SUM(active_hours), 0)
                )::float AS avg_precipitation_mm
            FROM filtered
            GROUP BY pu_location_id
        )
        SELECT
            *,
            (total_demand::float / NULLIF(SUM(total_demand) OVER (), 0)) AS demand_share,
            (total_demand::float / NULLIF(active_hours, 0)) AS avg_hourly_demand
        FROM aggregated
        ORDER BY total_demand DESC, pu_location_id
        LIMIT :limit
        """,
        start_month=start_month,
        end_month=end_month,
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
    return sorted(load_zone_lookup().values(), key=lambda zone: (zone["borough"], zone["zone_name"], zone["zone_id"]))


@app.get("/api/route-estimate")
def route_estimate(
    pickup_zone_id: int = Query(ge=1),
    dropoff_zone_id: int = Query(ge=1),
) -> dict[str, Any]:
    try:
        return calculate_zone_distance(pickup_zone_id, dropoff_zone_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


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
    pu_location_id: int = Field(ge=1)
    do_location_id: int = Field(ge=1)
    passenger_count: int = Field(ge=1, le=6)
    ratecode_id: int = Field(ge=1, le=5)
    hour: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=1, le=7)
    month: int = Field(ge=1, le=12)
    temperature_f: float
    precipitation_mm: float = Field(ge=0)
    payment_type: int = Field(ge=1, le=6)


@app.post("/api/predict/fare-tip")
def predict_fare_tip(req: FareTipPredictRequest) -> dict[str, Any]:
    try:
        route = calculate_zone_distance(req.pu_location_id, req.do_location_id)
        if not route["can_predict"]:
            raise ValueError(
                "Pickup and dropoff are the same zone; centroid distance is zero and cannot be used by the model"
            )
        features_df = build_prediction_features(
            trip_distance=route["trip_distance"],
            pu_location_id=req.pu_location_id,
            do_location_id=req.do_location_id,
            passenger_count=req.passenger_count,
            ratecode_id=req.ratecode_id,
            hour=req.hour,
            day_of_week=req.day_of_week,
            month=req.month,
            temperature_f=req.temperature_f,
            precipitation_mm=req.precipitation_mm,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    derived_features = {
        name: int(features_df.iloc[0][name])
        for name in ["is_rush_hour", "is_weekend", "is_raining", "is_cold"]
    }
    trip_distance = float(features_df.iloc[0]["trip_distance"])
    
    # Check if models are available
    if fare_model is not None or tip_model is not None:
        try:
            # Predict Fare
            if fare_model is not None:
                pred_fare = float(fare_model.predict(features_df)[0])
                pred_fare = max(0.0, round(pred_fare, 2))
            else:
                pred_fare = fallback_predict_fare(trip_distance, req.hour)
                
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
        pred_fare = fallback_predict_fare(trip_distance, req.hour)
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
        "trip_distance": trip_distance,
        "distance_method": route["distance_method"],
        "pickup_zone": route["pickup_zone"],
        "dropoff_zone": route["dropoff_zone"],
        "feature_columns": FARE_TIP_FEATURE_COLUMNS,
        "feature_values": {
            column: _json_value(features_df.iloc[0][column])
            for column in FARE_TIP_FEATURE_COLUMNS
        },
        "prediction_input": req.model_dump(),
        "derived_features": derived_features,
    }


@app.get("/api/zone-trend")
def zone_trend(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    limit: int = 5,
) -> list[dict[str, Any]]:
    filters = """
        WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
          AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
    """
    return fetch_all(
        f"""
        WITH top_zones AS (
            SELECT pu_location_id, pickup_zone
            FROM mart.dashboard_zone_summary
            {filters}
            GROUP BY pu_location_id, pickup_zone
            ORDER BY SUM(total_demand) DESC
            LIMIT :limit
        )
        SELECT s.pickup_year_month, t.pickup_zone, SUM(s.total_demand)::bigint AS total_demand
        FROM mart.dashboard_zone_summary s
        JOIN top_zones t ON s.pu_location_id = t.pu_location_id
        {filters}
        GROUP BY s.pickup_year_month, t.pickup_zone
        ORDER BY s.pickup_year_month ASC, total_demand DESC
        """,
        start_month=start_month,
        end_month=end_month,
        limit=limit,
    )


@app.get("/api/weather-correlation")
def weather_correlation(
    start_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
    end_month: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
) -> list[dict[str, Any]]:
    filters = """
        WHERE (CAST(:start_month AS varchar) IS NULL OR pickup_year_month >= CAST(:start_month AS varchar))
          AND (CAST(:end_month AS varchar) IS NULL OR pickup_year_month <= CAST(:end_month AS varchar))
    """
    return fetch_all(
        f"""
        SELECT 
            DATE(pickup_hour) AS pickup_date,
            SUM(total_demand)::bigint AS daily_demand,
            AVG(avg_temperature_f)::float AS temp_f,
            AVG(avg_precipitation_mm)::float AS precip_mm
        FROM mart.dashboard_hourly_demand_kpi
        {filters}
        GROUP BY DATE(pickup_hour)
        ORDER BY DATE(pickup_hour) ASC
        """,
        start_month=start_month,
        end_month=end_month,
    )
