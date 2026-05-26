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

GOLD_DEMAND_FEATURES = "GOLD_DEMAND_FEATURES"
GOLD_FARE_TIP_FEATURES = "GOLD_FARE_TIP_FEATURES"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

GOLD_ROOT_PATH = os.getenv("GOLD_ROOT_PATH", "s3a://gold")
GOLD_DEMAND_FEATURES_PATH = os.getenv(
    "GOLD_DEMAND_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_demand_features/"
)
GOLD_FARE_TIP_FEATURES_PATH = os.getenv(
    "GOLD_FARE_TIP_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_fare_tip_features/"
)
GOLD_QUALITY_REPORT_PATH = os.getenv(
    "GOLD_QUALITY_REPORT_PATH", f"{GOLD_ROOT_PATH}/quality_reports/gold_quality/latest/"
)

MIN_TIP_MODEL_FARE_AMOUNT = float(os.getenv("MIN_TIP_MODEL_FARE_AMOUNT", "2.5"))
MAX_FARE_MODEL_AMOUNT = float(os.getenv("MAX_FARE_MODEL_AMOUNT", "300.0"))
MAX_TIP_PERCENT = float(os.getenv("MAX_TIP_PERCENT", "100.0"))

DEMAND_EXPECTED_SCHEMA = {
    "pu_location_id": "smallint",
    "pickup_hour": "timestamp",
    "demand": "int",
    "hour": "tinyint",
    "day_of_week": "tinyint",
    "month": "tinyint",
    "temperature_f": "float",
    "precipitation_mm": "float",
    "pickup_year_month": "string",
    "gold_processed_timestamp": "timestamp",
}

FARE_TIP_EXPECTED_SCHEMA = {
    "fare_amount": "decimal(12,2)",
    "tip_amount": "decimal(12,2)",
    "tip_percent": "float",
    "trip_distance": "float",
    "pu_location_id": "smallint",
    "do_location_id": "smallint",
    "passenger_count": "smallint",
    "ratecode_id": "tinyint",
    "payment_type": "tinyint",
    "hour": "tinyint",
    "day_of_week": "tinyint",
    "month": "tinyint",
    "temperature_f": "float",
    "precipitation_mm": "float",
    "pickup_year_month": "string",
    "gold_processed_timestamp": "timestamp",
}

DEMAND_CRITICAL_NOT_NULL_COLUMNS = [
    "pu_location_id",
    "pickup_hour",
    "demand",
    "hour",
    "day_of_week",
    "month",
    "temperature_f",
    "precipitation_mm",
    "pickup_year_month",
]

FARE_TIP_CRITICAL_NOT_NULL_COLUMNS = [
    "fare_amount",
    "tip_amount",
    "tip_percent",
    "trip_distance",
    "pu_location_id",
    "do_location_id",
    "hour",
    "day_of_week",
    "month",
    "temperature_f",
    "precipitation_mm",
    "pickup_year_month",
]

FARE_TIP_KNOWN_NULLABLE_COLUMNS = [
    "passenger_count",
    "ratecode_id",
    "payment_type",
]


spark = (
    SparkSession.builder.appName("MetroPulse_Gold_Quality_Check")
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


def metric(dataset_name, dataset_path, name, value, status="pass", details=""):
    return Row(
        dataset_name=dataset_name,
        dataset_path=dataset_path,
        check_name=name,
        check_value=str(value),
        status=status,
        details=details,
    )


def schema_metrics(df, dataset_name, dataset_path, expected_schema):
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    rows = []

    for column_name, expected_type in expected_schema.items():
        actual_type = actual_schema.get(column_name)
        status = "pass" if actual_type == expected_type else "fail"
        rows.append(
            metric(
                dataset_name,
                dataset_path,
                f"schema_{column_name}",
                actual_type,
                status,
                f"expected={expected_type}",
            )
        )

    extra_columns = sorted(set(actual_schema) - set(expected_schema))
    missing_columns = sorted(set(expected_schema) - set(actual_schema))
    rows.append(metric(dataset_name, dataset_path, "schema_extra_columns", extra_columns, "pass"))
    rows.append(
        metric(
            dataset_name,
            dataset_path,
            "schema_missing_columns",
            missing_columns,
            "pass" if not missing_columns else "fail",
        )
    )
    return rows


def null_metrics(df, dataset_name, dataset_path, total_rows, critical_columns, nullable_columns=None):
    nullable_columns = nullable_columns or []
    columns_to_check = critical_columns + nullable_columns

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
    for column_name in critical_columns:
        null_count = int(null_counts[column_name] or 0)
        rows.append(
            metric(
                dataset_name,
                dataset_path,
                f"critical_null_{column_name}",
                null_count,
                "pass" if null_count == 0 else "fail",
                f"ratio={(null_count / total_rows) if total_rows else 0:.8f}",
            )
        )

    for column_name in nullable_columns:
        null_count = int(null_counts[column_name] or 0)
        rows.append(
            metric(
                dataset_name,
                dataset_path,
                f"known_nullable_null_{column_name}",
                null_count,
                "pass",
                f"ratio={(null_count / total_rows) if total_rows else 0:.8f}",
            )
        )

    return rows


def demand_key_metrics(df, total_rows):
    duplicate_groups = (
        df.groupBy("pu_location_id", "pickup_hour")
        .agg(spark_count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    return [
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "duplicate_pu_location_pickup_hour_groups",
            duplicate_groups,
            "pass" if duplicate_groups == 0 else "fail",
        ),
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "demand_row_count",
            total_rows,
            "pass" if total_rows > 0 else "fail",
        ),
    ]


def demand_range_metrics(df):
    profile = df.select(
        spark_min("pickup_hour").alias("min_pickup_hour"),
        spark_max("pickup_hour").alias("max_pickup_hour"),
        spark_sum(when(col("demand") <= 0, 1).otherwise(0)).alias("non_positive_demand_rows"),
        spark_sum(when((col("hour") < 0) | (col("hour") > 23), 1).otherwise(0)).alias(
            "invalid_hour_rows"
        ),
        spark_sum(
            when((col("day_of_week") < 1) | (col("day_of_week") > 7), 1).otherwise(0)
        ).alias("invalid_day_of_week_rows"),
        spark_sum(when((col("month") < 1) | (col("month") > 12), 1).otherwise(0)).alias(
            "invalid_month_rows"
        ),
    ).collect()[0]

    return [
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "min_pickup_hour",
            profile["min_pickup_hour"],
            "pass",
        ),
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "max_pickup_hour",
            profile["max_pickup_hour"],
            "pass",
        ),
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "non_positive_demand_rows",
            int(profile["non_positive_demand_rows"] or 0),
            "pass" if int(profile["non_positive_demand_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "invalid_hour_rows",
            int(profile["invalid_hour_rows"] or 0),
            "pass" if int(profile["invalid_hour_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "invalid_day_of_week_rows",
            int(profile["invalid_day_of_week_rows"] or 0),
            "pass" if int(profile["invalid_day_of_week_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            "invalid_month_rows",
            int(profile["invalid_month_rows"] or 0),
            "pass" if int(profile["invalid_month_rows"] or 0) == 0 else "fail",
        ),
    ]


def fare_tip_range_metrics(df, total_rows):
    profile = df.select(
        spark_sum(when(col("fare_amount") <= 0, 1).otherwise(0)).alias(
            "non_positive_fare_rows"
        ),
        spark_sum(when(col("fare_amount") < lit(MIN_TIP_MODEL_FARE_AMOUNT), 1).otherwise(0)).alias(
            "below_min_tip_model_fare_rows"
        ),
        spark_sum(when(col("fare_amount") > lit(MAX_FARE_MODEL_AMOUNT), 1).otherwise(0)).alias(
            "above_max_fare_model_amount_rows"
        ),
        spark_sum(when(col("trip_distance") <= 0, 1).otherwise(0)).alias(
            "non_positive_distance_rows"
        ),
        spark_sum(when(col("tip_percent") < 0, 1).otherwise(0)).alias(
            "negative_tip_percent_rows"
        ),
        spark_sum(when(col("tip_percent") > lit(MAX_TIP_PERCENT), 1).otherwise(0)).alias(
            "high_tip_percent_rows"
        ),
        spark_sum(when((col("hour") < 0) | (col("hour") > 23), 1).otherwise(0)).alias(
            "invalid_hour_rows"
        ),
        spark_sum(
            when((col("day_of_week") < 1) | (col("day_of_week") > 7), 1).otherwise(0)
        ).alias("invalid_day_of_week_rows"),
        spark_sum(when((col("month") < 1) | (col("month") > 12), 1).otherwise(0)).alias(
            "invalid_month_rows"
        ),
        spark_sum(when(col("payment_type") == 1, 1).otherwise(0)).alias(
            "credit_card_payment_rows"
        ),
    ).collect()[0]

    credit_card_rows = int(profile["credit_card_payment_rows"] or 0)
    return [
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "fare_tip_row_count",
            total_rows,
            "pass" if total_rows > 0 else "fail",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "non_positive_fare_rows",
            int(profile["non_positive_fare_rows"] or 0),
            "pass" if int(profile["non_positive_fare_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "below_min_tip_model_fare_rows",
            int(profile["below_min_tip_model_fare_rows"] or 0),
            "pass" if int(profile["below_min_tip_model_fare_rows"] or 0) == 0 else "fail",
            f"minimum={MIN_TIP_MODEL_FARE_AMOUNT}",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "above_max_fare_model_amount_rows",
            int(profile["above_max_fare_model_amount_rows"] or 0),
            "pass" if int(profile["above_max_fare_model_amount_rows"] or 0) == 0 else "fail",
            f"maximum={MAX_FARE_MODEL_AMOUNT}",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "non_positive_distance_rows",
            int(profile["non_positive_distance_rows"] or 0),
            "pass" if int(profile["non_positive_distance_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "negative_tip_percent_rows",
            int(profile["negative_tip_percent_rows"] or 0),
            "pass" if int(profile["negative_tip_percent_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "high_tip_percent_rows",
            int(profile["high_tip_percent_rows"] or 0),
            "pass" if int(profile["high_tip_percent_rows"] or 0) == 0 else "fail",
            f"maximum={MAX_TIP_PERCENT}",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "invalid_hour_rows",
            int(profile["invalid_hour_rows"] or 0),
            "pass" if int(profile["invalid_hour_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "invalid_day_of_week_rows",
            int(profile["invalid_day_of_week_rows"] or 0),
            "pass" if int(profile["invalid_day_of_week_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "invalid_month_rows",
            int(profile["invalid_month_rows"] or 0),
            "pass" if int(profile["invalid_month_rows"] or 0) == 0 else "fail",
        ),
        metric(
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            "credit_card_payment_rows",
            credit_card_rows,
            "pass",
            "Use payment_type = 1 subset when training tip_percent.",
        ),
    ]


def write_report(metrics):
    report_df = spark.createDataFrame(metrics).withColumn("checked_at", current_timestamp())
    logger.info("Writing Gold quality report to %s", GOLD_QUALITY_REPORT_PATH)
    report_df.coalesce(1).write.mode("overwrite").json(GOLD_QUALITY_REPORT_PATH)


def main():
    logger.info("Reading %s from %s", GOLD_DEMAND_FEATURES, GOLD_DEMAND_FEATURES_PATH)
    demand_features = spark.read.parquet(GOLD_DEMAND_FEATURES_PATH)

    logger.info("Reading %s from %s", GOLD_FARE_TIP_FEATURES, GOLD_FARE_TIP_FEATURES_PATH)
    fare_tip_features = spark.read.parquet(GOLD_FARE_TIP_FEATURES_PATH)

    metrics = []

    metrics.extend(
        schema_metrics(
            demand_features,
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            DEMAND_EXPECTED_SCHEMA,
        )
    )
    demand_total_rows = demand_features.count()
    metrics.extend(
        null_metrics(
            demand_features,
            GOLD_DEMAND_FEATURES,
            GOLD_DEMAND_FEATURES_PATH,
            demand_total_rows,
            DEMAND_CRITICAL_NOT_NULL_COLUMNS,
        )
    )
    metrics.extend(demand_key_metrics(demand_features, demand_total_rows))
    metrics.extend(demand_range_metrics(demand_features))

    metrics.extend(
        schema_metrics(
            fare_tip_features,
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            FARE_TIP_EXPECTED_SCHEMA,
        )
    )
    fare_tip_total_rows = fare_tip_features.count()
    metrics.extend(
        null_metrics(
            fare_tip_features,
            GOLD_FARE_TIP_FEATURES,
            GOLD_FARE_TIP_FEATURES_PATH,
            fare_tip_total_rows,
            FARE_TIP_CRITICAL_NOT_NULL_COLUMNS,
            FARE_TIP_KNOWN_NULLABLE_COLUMNS,
        )
    )
    metrics.extend(fare_tip_range_metrics(fare_tip_features, fare_tip_total_rows))

    write_report(metrics)

    failed = [row for row in metrics if row.status == "fail"]
    if failed:
        for row in failed:
            logger.error(
                "QUALITY CHECK FAILED: %s.%s=%s %s",
                row.dataset_name,
                row.check_name,
                row.check_value,
                row.details,
            )
        raise RuntimeError(f"Gold quality check failed with {len(failed)} failing checks")

    logger.info("Gold quality check passed with %s checks", len(metrics))


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
