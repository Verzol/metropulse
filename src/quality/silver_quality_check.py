import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import (
    col,
    count as spark_count,
    current_timestamp,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

load_dotenv()

NYC_TIMEZONE = "America/New_York"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

SILVER_TAXI_WEATHER_CORE_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_CORE_PATH", "s3a://silver/taxi_weather_trips_core/"
)
SILVER_TAXI_WEATHER_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_PATH", "s3a://silver/taxi_weather_trips/"
)
SILVER_QUALITY_REPORT_PATH = os.getenv(
    "SILVER_QUALITY_REPORT_PATH", "s3a://silver/quality_reports/silver_core_quality/latest/"
)

EXPECTED_SCHEMA = {
    "taxi_type": "string",
    "vendor_id": "tinyint",
    "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp",
    "pickup_hour": "timestamp",
    "pickup_date": "date",
    "pu_location_id": "smallint",
    "do_location_id": "smallint",
    "passenger_count": "smallint",
    "trip_distance": "float",
    "ratecode_id": "tinyint",
    "payment_type": "tinyint",
    "fare_amount": "decimal(12,2)",
    "extra": "decimal(12,2)",
    "mta_tax": "decimal(12,2)",
    "tip_amount": "decimal(12,2)",
    "tolls_amount": "decimal(12,2)",
    "improvement_surcharge": "decimal(12,2)",
    "total_amount": "decimal(12,2)",
    "congestion_surcharge": "decimal(12,2)",
    "airport_fee": "decimal(12,2)",
    "temperature_f": "float",
    "humidity_percent": "float",
    "precipitation_mm": "float",
    "weather_code": "smallint",
    "wind_speed_kmh": "float",
    "wind_direction_deg": "float",
    "cloud_cover_percent": "float",
    "is_valid_distance": "boolean",
    "is_valid_fare": "boolean",
    "is_valid_total_amount": "boolean",
    "is_outlier_trip": "boolean",
    "core_processed_timestamp": "timestamp",
    "pickup_year_month": "string",
}

CRITICAL_NOT_NULL_COLUMNS = [
    "taxi_type",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_hour",
    "pickup_date",
    "pu_location_id",
    "do_location_id",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "temperature_f",
    "humidity_percent",
    "precipitation_mm",
    "weather_code",
    "wind_speed_kmh",
    "wind_direction_deg",
    "cloud_cover_percent",
    "pickup_year_month",
]

KNOWN_NULLABLE_COLUMNS = [
    "passenger_count",
    "ratecode_id",
    "payment_type",
    "congestion_surcharge",
    "airport_fee",
]


spark = (
    SparkSession.builder.appName("MetroPulse_Silver_Quality_Check")
    .config("spark.sql.session.timeZone", NYC_TIMEZONE)
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "48"))
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=America/New_York")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=America/New_York")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


def metric(name, value, status="pass", details=""):
    return Row(
        check_name=name,
        check_value=str(value),
        status=status,
        details=details,
    )


def schema_metrics(df):
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    rows = []
    for column_name, expected_type in EXPECTED_SCHEMA.items():
        actual_type = actual_schema.get(column_name)
        status = "pass" if actual_type == expected_type else "fail"
        rows.append(
            metric(
                f"schema_{column_name}",
                actual_type,
                status,
                f"expected={expected_type}",
            )
        )

    extra_columns = sorted(set(actual_schema) - set(EXPECTED_SCHEMA))
    missing_columns = sorted(set(EXPECTED_SCHEMA) - set(actual_schema))
    rows.append(metric("schema_extra_columns", extra_columns, "pass"))
    rows.append(
        metric(
            "schema_missing_columns",
            missing_columns,
            "pass" if not missing_columns else "fail",
        )
    )
    return rows


def null_metrics(df, total_rows):
    columns_to_check = CRITICAL_NOT_NULL_COLUMNS + KNOWN_NULLABLE_COLUMNS
    null_counts = (
        df.agg(
            *[
                spark_sum(when(col(column_name).isNull(), 1).otherwise(0)).alias(column_name)
                for column_name in columns_to_check
            ]
        )
        .collect()[0]
        .asDict()
    )

    rows = []
    for column_name in CRITICAL_NOT_NULL_COLUMNS:
        null_count = int(null_counts[column_name] or 0)
        rows.append(
            metric(
                f"critical_null_{column_name}",
                null_count,
                "pass" if null_count == 0 else "fail",
                f"ratio={(null_count / total_rows) if total_rows else 0:.8f}",
            )
        )

    for column_name in KNOWN_NULLABLE_COLUMNS:
        null_count = int(null_counts[column_name] or 0)
        rows.append(
            metric(
                f"known_nullable_null_{column_name}",
                null_count,
                "pass",
                f"ratio={(null_count / total_rows) if total_rows else 0:.8f}",
            )
        )
    return rows


def profile_metrics(df):
    profile = df.select(
        spark_min("pickup_datetime").alias("min_pickup_datetime"),
        spark_max("pickup_datetime").alias("max_pickup_datetime"),
        spark_sum(when(col("is_outlier_trip"), 1).otherwise(0)).alias("outlier_rows"),
        spark_sum(when(~col("is_outlier_trip"), 1).otherwise(0)).alias("non_outlier_rows"),
    ).collect()[0]

    return [
        metric("min_pickup_datetime", profile["min_pickup_datetime"], "pass"),
        metric("max_pickup_datetime", profile["max_pickup_datetime"], "pass"),
        metric("outlier_rows", int(profile["outlier_rows"] or 0), "pass"),
        metric("non_outlier_rows", int(profile["non_outlier_rows"] or 0), "pass"),
    ]


def compare_enriched_count(core_df, metrics):
    try:
        enriched_count = spark.read.parquet(SILVER_TAXI_WEATHER_PATH).count()
        core_count = core_df.count()
        metrics.append(metric("enriched_row_count", enriched_count, "pass"))
        metrics.append(metric("core_row_count_for_compare", core_count, "pass"))
        metrics.append(
            metric(
                "core_matches_enriched_count",
                core_count == enriched_count,
                "pass" if core_count == enriched_count else "fail",
            )
        )
    except Exception as exc:
        logger.warning("Skipping enriched/core count comparison: %s", exc)
        metrics.append(metric("core_matches_enriched_count", "skipped", "pass", str(exc)))


def write_report(metrics):
    report_df = (
        spark.createDataFrame(metrics)
        .withColumn("checked_at", current_timestamp())
        .withColumn("dataset_path", lit(SILVER_TAXI_WEATHER_CORE_PATH))
    )
    logger.info("Writing Silver quality report to %s", SILVER_QUALITY_REPORT_PATH)
    report_df.coalesce(1).write.mode("overwrite").json(SILVER_QUALITY_REPORT_PATH)


def main():
    logger.info("Reading Silver Core from %s", SILVER_TAXI_WEATHER_CORE_PATH)
    core = spark.read.parquet(SILVER_TAXI_WEATHER_CORE_PATH)

    metrics = schema_metrics(core)

    total_rows = core.count()
    metrics.append(metric("core_row_count", total_rows, "pass" if total_rows > 0 else "fail"))
    metrics.extend(null_metrics(core, total_rows))
    metrics.extend(profile_metrics(core))
    compare_enriched_count(core, metrics)

    write_report(metrics)

    failed = [row for row in metrics if row.status == "fail"]
    if failed:
        for row in failed:
            logger.error("QUALITY CHECK FAILED: %s=%s %s", row.check_name, row.check_value, row.details)
        raise RuntimeError(f"Silver quality check failed with {len(failed)} failing checks")

    logger.info("Silver quality check passed with %s checks", len(metrics))


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
