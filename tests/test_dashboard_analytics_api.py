import unittest
from unittest.mock import patch

from src.dashboard_api import main


class DashboardAnalyticsApiTests(unittest.TestCase):
    def test_summary_uses_full_period_and_weighted_payment_metrics(self):
        hourly = {
            "total_demand": 1000,
            "hourly_points": 10,
            "weekend_demand": 250,
            "rush_hour_demand": 400,
            "last_data_hour": "2024-12-31T23:00:00",
        }
        peak = {"peak_hour": "2024-12-18T18:00:00", "peak_total_demand": 120}
        payment = {
            "fare_tip_trip_count": 800,
            "avg_fare_amount": 20.5,
            "avg_tip_percent": 7.2,
        }
        leader = {"payment_type": 1, "trip_count": 600}

        captured_sql = []

        def fake_fetch_one(sql, **params):
            captured_sql.append(sql)
            return [hourly, peak, payment, leader][len(captured_sql) - 1]

        with patch.object(main, "fetch_one", side_effect=fake_fetch_one):
            result = main.summary("2023-01", "2024-12")

        self.assertEqual(result["weekend_share"], 0.25)
        self.assertEqual(result["rush_hour_share"], 0.4)
        self.assertEqual(result["leading_payment_share"], 0.75)
        self.assertIn("SUM(avg_fare_amount * trip_count)", captured_sql[2])
        self.assertIn("SUM(avg_tip_percent * trip_count)", captured_sql[2])
        self.assertIn("EXTRACT(ISODOW FROM pickup_hour)", captured_sql[0])

    def test_demand_trends_filters_every_aggregation(self):
        captured = []

        def fake_fetch_all(sql, **params):
            captured.append((sql, params))
            return []

        with patch.object(main, "fetch_all", side_effect=fake_fetch_all):
            result = main.demand_trends("2024-01", "2024-03")

        self.assertEqual(result, {"monthly": [], "hourly": [], "weekday": []})
        self.assertEqual(len(captured), 3)
        for sql, params in captured:
            self.assertIn("pickup_year_month >=", sql)
            self.assertEqual(params["start_month"], "2024-01")
            self.assertEqual(params["end_month"], "2024-03")

    def test_zone_summary_share_is_computed_before_limit(self):
        captured = {}

        def fake_fetch_all(sql, **params):
            captured["sql"] = sql
            captured["params"] = params
            return []

        with patch.object(main, "fetch_all", side_effect=fake_fetch_all):
            main.zone_summary("2024-01", "2024-06", 10)

        self.assertIn("SUM(total_demand) OVER ()", captured["sql"])
        self.assertIn("GROUP BY pu_location_id", captured["sql"])
        self.assertIn("LIMIT :limit", captured["sql"])
        self.assertEqual(captured["params"]["limit"], 10)
        self.assertEqual(captured["params"]["start_month"], "2024-01")

    def test_hourly_pagination_returns_metadata_without_affecting_summary(self):
        rows = [{"pickup_hour": "2024-01-01T00:00:00", "total_demand": 10}]
        with (
            patch.object(main, "fetch_one", return_value={"total_rows": 17542}),
            patch.object(main, "fetch_all", return_value=rows) as fetch_all,
        ):
            result = main.hourly_demand("2023-01", "2024-12", 200, 400)

        self.assertEqual(result["total_rows"], 17542)
        self.assertEqual(result["limit"], 200)
        self.assertEqual(result["offset"], 400)
        self.assertEqual(result["rows"], rows)
        self.assertEqual(fetch_all.call_args.kwargs["offset"], 400)


if __name__ == "__main__":
    unittest.main()
