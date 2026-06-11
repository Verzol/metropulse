import json
import unittest
from pathlib import Path

from fastapi import HTTPException

from src.dashboard_api.main import FareTipPredictRequest, predict_fare_tip
from src.dashboard_api.prediction import (
    FARE_TIP_FEATURE_COLUMNS,
    build_prediction_features,
    calculate_zone_distance,
    load_zone_lookup,
)


class DashboardPredictionTests(unittest.TestCase):
    def test_official_zone_lookup_has_expected_landmarks(self):
        zones = load_zone_lookup()
        self.assertEqual(len(zones), 263)
        self.assertEqual(zones[132]["zone_name"], "JFK Airport")
        self.assertEqual(zones[138]["zone_name"], "LaGuardia Airport")
        self.assertEqual(zones[161]["zone_name"], "Midtown Center")

    def test_distance_changes_with_dropoff_zone(self):
        jfk_to_midtown = calculate_zone_distance(132, 161)
        jfk_to_laguardia = calculate_zone_distance(132, 138)
        self.assertTrue(jfk_to_midtown["can_predict"])
        self.assertGreater(jfk_to_midtown["trip_distance"], 0)
        self.assertNotEqual(
            jfk_to_midtown["trip_distance"],
            jfk_to_laguardia["trip_distance"],
        )

    def test_same_zone_is_not_predictable(self):
        route = calculate_zone_distance(132, 132)
        self.assertEqual(route["trip_distance"], 0)
        self.assertTrue(route["same_zone"])
        self.assertFalse(route["can_predict"])

    def test_feature_frame_matches_saved_model_schema(self):
        route = calculate_zone_distance(132, 161)
        features = build_prediction_features(
            trip_distance=route["trip_distance"],
            pu_location_id=132,
            do_location_id=161,
            passenger_count=2,
            ratecode_id=2,
            hour=8,
            day_of_week=2,
            month=12,
            temperature_f=34.0,
            precipitation_mm=0.5,
        )
        self.assertEqual(list(features.columns), FARE_TIP_FEATURE_COLUMNS)
        self.assertEqual(features.iloc[0]["day_of_week"], 2)
        self.assertEqual(features.iloc[0]["is_rush_hour"], 1)
        self.assertEqual(features.iloc[0]["is_raining"], 1)

        project_root = Path(__file__).resolve().parents[1]
        for model_name in ["fare_xgb.json", "tip_xgb.json"]:
            payload = json.loads((project_root / "ml" / "models" / model_name).read_text())
            self.assertEqual(payload["learner"]["feature_names"], FARE_TIP_FEATURE_COLUMNS)

    def test_prediction_endpoint_calculates_distance_server_side(self):
        result = predict_fare_tip(
            FareTipPredictRequest(
                pu_location_id=132,
                do_location_id=161,
                passenger_count=1,
                ratecode_id=2,
                hour=8,
                day_of_week=2,
                month=12,
                temperature_f=34.0,
                precipitation_mm=0.0,
                payment_type=1,
            )
        )
        self.assertGreater(result["trip_distance"], 0)
        self.assertEqual(result["feature_columns"], FARE_TIP_FEATURE_COLUMNS)
        self.assertEqual(result["feature_values"]["trip_distance"], result["trip_distance"])
        self.assertGreaterEqual(result["predicted_fare"], 0)

    def test_prediction_rejects_same_zone(self):
        with self.assertRaises(HTTPException) as context:
            predict_fare_tip(
                FareTipPredictRequest(
                    pu_location_id=132,
                    do_location_id=132,
                    passenger_count=1,
                    ratecode_id=1,
                    hour=8,
                    day_of_week=2,
                    month=12,
                    temperature_f=34.0,
                    precipitation_mm=0.0,
                    payment_type=1,
                )
            )
        self.assertEqual(context.exception.status_code, 422)

    def test_fare_changes_with_route_and_cash_tip_is_zero(self):
        common = {
            "passenger_count": 1,
            "ratecode_id": 1,
            "hour": 8,
            "day_of_week": 2,
            "month": 12,
            "temperature_f": 34.0,
            "precipitation_mm": 0.0,
        }
        midtown_result = predict_fare_tip(
            FareTipPredictRequest(
                pu_location_id=132,
                do_location_id=161,
                payment_type=1,
                **common,
            )
        )
        laguardia_result = predict_fare_tip(
            FareTipPredictRequest(
                pu_location_id=132,
                do_location_id=138,
                payment_type=1,
                **common,
            )
        )
        cash_result = predict_fare_tip(
            FareTipPredictRequest(
                pu_location_id=132,
                do_location_id=161,
                payment_type=2,
                **common,
            )
        )

        self.assertNotEqual(midtown_result["trip_distance"], laguardia_result["trip_distance"])
        self.assertNotEqual(midtown_result["predicted_fare"], laguardia_result["predicted_fare"])
        self.assertEqual(cash_result["predicted_tip_percent"], 0)
        self.assertEqual(cash_result["predicted_tip_amount"], 0)


if __name__ == "__main__":
    unittest.main()
