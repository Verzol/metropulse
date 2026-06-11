import unittest
from datetime import datetime


try:
    from pyspark.sql import SparkSession

    from src.processing.gold_dashboard_marts import build_zone_summary
except ImportError:
    SparkSession = None
    build_zone_summary = None


@unittest.skipIf(SparkSession is None, "pyspark is not installed")
class GoldDashboardMartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.spark = (
                SparkSession.builder.master("local[1]")
                .appName("gold-dashboard-mart-tests")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
        except Exception as exc:
            raise unittest.SkipTest(f"local Spark is unavailable: {exc}") from exc

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_zone_summary_has_month_zone_grain_and_preserves_demand(self):
        processed_at = datetime(2025, 1, 1)
        demand = self.spark.createDataFrame(
            [
                ("2024-01", 1, 10, datetime(2024, 1, 1), 30.0, 0.0, processed_at),
                ("2024-01", 1, 20, datetime(2024, 1, 2), 32.0, 1.0, processed_at),
                ("2024-01", 2, 5, datetime(2024, 1, 1), 31.0, 0.0, processed_at),
                ("2024-02", 1, 7, datetime(2024, 2, 1), 35.0, 0.0, processed_at),
            ],
            [
                "pickup_year_month",
                "pu_location_id",
                "demand",
                "pickup_hour",
                "temperature_f",
                "precipitation_mm",
                "gold_processed_timestamp",
            ],
        )
        zones = self.spark.createDataFrame(
            [
                (1, "EWR", "Newark Airport", 40.69, -74.17),
                (2, "Queens", "Jamaica Bay", 40.61, -73.84),
            ],
            [
                "pu_location_id",
                "pickup_borough",
                "pickup_zone",
                "pickup_latitude",
                "pickup_longitude",
            ],
        )

        result = build_zone_summary(demand, zones)
        keys = {
            (row["pickup_year_month"], row["pu_location_id"])
            for row in result.select("pickup_year_month", "pu_location_id").collect()
        }

        self.assertEqual(keys, {("2024-01", 1), ("2024-01", 2), ("2024-02", 1)})
        self.assertEqual(result.count(), len(keys))
        self.assertEqual(
            result.agg({"total_demand": "sum"}).collect()[0][0],
            demand.agg({"demand": "sum"}).collect()[0][0],
        )


if __name__ == "__main__":
    unittest.main()
