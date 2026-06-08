import os
import pandas as pd
from sqlalchemy import URL, create_engine, text


def get_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["ML_DB_USER"],
        password=os.environ["ML_DB_PASSWORD"],
        host="127.0.0.1",
        port=5433,
        database="metropulse_dw",
    )
    return create_engine(url)


def load_demand_features(engine, limit: int = None) -> pd.DataFrame:
    """
    Load ml.gold_demand_features_utc_fix từ PostgreSQL.
    Đảm bảo lấy pickup_hour để phục vụ việc bóc tách dữ liệu theo mốc thời gian.
    """
    query = """
        SELECT
            pu_location_id,
            to_char(pickup_hour, 'YYYY-MM-DD HH24:MI:SS') AS pickup_hour,
            demand,
            hour,
            day_of_week,
            month,
            temperature_f,
            precipitation_mm
        FROM ml.gold_demand_features_utc_fix
        ORDER BY pickup_hour, pu_location_id
    """
    if limit:
        query += f" LIMIT {limit}"

    df = pd.read_sql(text(query), engine)
    
    df["pickup_hour"] = pd.to_datetime(df["pickup_hour"], format="%Y-%m-%d %H:%M:%S")
        
    return df


def load_fare_tip_features(
    engine,
    sample_pct: int = 5,
) -> pd.DataFrame:
    pct = max(1, min(100, int(sample_pct)))
    query = f"""
        SELECT
            fare_amount,
            tip_amount,
            tip_percent,
            trip_distance,
            pu_location_id,
            do_location_id,
            passenger_count,
            ratecode_id,
            payment_type,
            hour,
            day_of_week,
            month,
            temperature_f,
            precipitation_mm
        FROM ml.gold_fare_tip_features
        WHERE abs(hashtext(
            pu_location_id::text
            || hour::text
            || trip_distance::text
        )) % 100 < {pct}
    """
    df = pd.read_sql(text(query), engine)
    return df
