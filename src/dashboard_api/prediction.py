import csv
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ZONE_LOOKUP_PATH = PROJECT_ROOT / "data" / "taxi_zone_centroids.csv"
EARTH_RADIUS_MILES = 3958.7613

FARE_TIP_FEATURE_COLUMNS = [
    "trip_distance",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "ratecode_id",
    "hour",
    "day_of_week",
    "month",
    "temperature_f",
    "precipitation_mm",
    "is_rush_hour",
    "is_weekend",
    "is_raining",
    "is_cold",
]


@lru_cache(maxsize=1)
def load_zone_lookup() -> dict[int, dict[str, Any]]:
    if not ZONE_LOOKUP_PATH.exists():
        raise FileNotFoundError(f"Zone lookup not found: {ZONE_LOOKUP_PATH}")

    zones: dict[int, dict[str, Any]] = {}
    with ZONE_LOOKUP_PATH.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            zone_id = int(row["LocationID"])
            zones[zone_id] = {
                "zone_id": zone_id,
                "zone_name": row["Zone"],
                "borough": row["Borough"],
                "latitude": float(row["Latitude"]) if row["Latitude"] else None,
                "longitude": float(row["Longitude"]) if row["Longitude"] else None,
            }
    return zones


def get_zone_centroid(zone_id: int) -> tuple[float, float] | None:
    zone = load_zone_lookup().get(int(zone_id))
    if not zone or zone["latitude"] is None or zone["longitude"] is None:
        return None
    return float(zone["latitude"]), float(zone["longitude"])


def haversine_miles(
    origin_latitude: float,
    origin_longitude: float,
    destination_latitude: float,
    destination_longitude: float,
) -> float:
    lat1, lon1, lat2, lon2 = map(
        math.radians,
        [origin_latitude, origin_longitude, destination_latitude, destination_longitude],
    )
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_MILES * math.asin(math.sqrt(haversine))


def calculate_zone_distance(pickup_zone_id: int, dropoff_zone_id: int) -> dict[str, Any]:
    zones = load_zone_lookup()
    pickup = zones.get(int(pickup_zone_id))
    dropoff = zones.get(int(dropoff_zone_id))
    if pickup is None:
        raise ValueError(f"Unknown pickup zone ID: {pickup_zone_id}")
    if dropoff is None:
        raise ValueError(f"Unknown dropoff zone ID: {dropoff_zone_id}")

    pickup_centroid = get_zone_centroid(pickup_zone_id)
    dropoff_centroid = get_zone_centroid(dropoff_zone_id)
    if pickup_centroid is None or dropoff_centroid is None:
        raise ValueError("Pickup or dropoff zone is missing centroid coordinates")

    distance = haversine_miles(*pickup_centroid, *dropoff_centroid)
    same_zone = int(pickup_zone_id) == int(dropoff_zone_id)
    return {
        "pickup_zone": pickup,
        "dropoff_zone": dropoff,
        "trip_distance": round(distance, 3),
        "distance_method": "haversine_zone_centroid",
        "same_zone": same_zone,
        "can_predict": not same_zone and distance > 0,
    }


def build_prediction_features(
    *,
    trip_distance: float,
    pu_location_id: int,
    do_location_id: int,
    passenger_count: int,
    ratecode_id: int,
    hour: int,
    day_of_week: int,
    month: int,
    temperature_f: float,
    precipitation_mm: float,
) -> pd.DataFrame:
    if trip_distance <= 0 or trip_distance >= 150:
        raise ValueError("trip_distance must be greater than 0 and less than 150 miles")

    # Keep derived flags aligned with the existing fare/tip model artifact.
    row = {
        "trip_distance": float(trip_distance),
        "pu_location_id": int(pu_location_id),
        "do_location_id": int(do_location_id),
        "passenger_count": float(passenger_count),
        "ratecode_id": float(ratecode_id),
        "hour": int(hour),
        "day_of_week": int(day_of_week),
        "month": int(month),
        "temperature_f": float(temperature_f),
        "precipitation_mm": float(precipitation_mm),
        "is_rush_hour": int(hour in [7, 8, 9, 17, 18, 19]),
        "is_weekend": int(day_of_week in [5, 6]),
        "is_raining": int(precipitation_mm > 0.0),
        "is_cold": int(temperature_f < 36.0),
    }
    return pd.DataFrame([row], columns=FARE_TIP_FEATURE_COLUMNS)
