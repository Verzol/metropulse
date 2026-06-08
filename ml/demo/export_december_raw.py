# demo/export_december_raw.py
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from sqlalchemy import text
from db import get_engine

os.makedirs("data_demo", exist_ok=True)

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
    WHERE pickup_hour >= '2024-12-01' AND pickup_hour < '2025-01-01'
    ORDER BY pickup_hour, pu_location_id
""")
df = pd.read_sql(query, engine)
df.to_csv("demo/data_demo/demand_december_raw.csv", index=False)
print(f"✅ Exported {len(df)} rows")