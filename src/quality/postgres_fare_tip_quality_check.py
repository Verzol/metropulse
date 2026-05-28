import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import count, countDistinct, current_timestamp, max as spark_max, min as spark_min, sum as spark_sum, when


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

load_dotenv()

NYC_TIMEZONE = "America/New_York"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
GOLD_ROOT_PATH = os.getenv("GOLD_ROOT_PATH", "s3a://gold")
GOLD_FARE_TIP_FEATURES_PATH = os.getenv(
    "GOLD_FARE_TIP_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_fare_tip_features/"
)

WAREHOUSE_JDBC_URL = os.getenv(
    "WAREHOUSE_JDBC_URL", "jdbc:postgresql://warehouse-postgres:5432/metropulse_dw"
)
WAREHOUSE_POSTGRES_USER = os.getenv("WAREHOUSE_POSTGRES_USER")
WAREHOUSE_POSTGRES_PASSWORD = os.getenv("WAREHOUSE_POSTGRES_PASSWORD")

SERVING_TABLE = "ml.gold_fare_tip_features"
VALIDATION_TABLE = "audit.validation_results"

spark = (
    SparkSession.builder.appName("MetroPulse_Postgres_Fare_Tip_Quality_Check")
    .config("spark.sql.session.timeZone", NYC_TIMEZONE)
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "48"))
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=America/New_York")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=America/New_York")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


def jdbc_reader(table_name):
    return (
        spark.read.format("jdbc")
        .option("url", WAREHOUSE_JDBC_URL)
        .option("dbtable", table_name)
        .option("user", WAREHOUSE_POSTGRES_USER)
        .option("password", WAREHOUSE_POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def fare_tip_metrics(df):
    row = df.agg(
        count("*").alias("row_count"),
        spark_sum("fare_amount").alias("total_fare_amount"),
        spark_sum("tip_amount").alias("total_tip_amount"),
        spark_sum(when(df.payment_type == 1, 1).otherwise(0)).alias("credit_card_payment_rows"),
        countDistinct("pickup_year_month").alias("pickup_year_month_count"),
        spark_min("gold_processed_timestamp").alias("min_gold_processed_timestamp"),
        spark_max("gold_processed_timestamp").alias("max_gold_processed_timestamp"),
    ).collect()[0]
    return {field: str(row[field]) for field in row.asDict()}


def postgres_fare_tip_metrics(table_name):
    query = (
        "(SELECT COUNT(*) AS row_count, "
        "CAST(SUM(fare_amount) AS NUMERIC(38, 2)) AS total_fare_amount, "
        "CAST(SUM(tip_amount) AS NUMERIC(38, 2)) AS total_tip_amount, "
        "SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_payment_rows, "
        "COUNT(DISTINCT pickup_year_month) AS pickup_year_month_count, "
        "MIN(gold_processed_timestamp) AS min_gold_processed_timestamp, "
        "MAX(gold_processed_timestamp) AS max_gold_processed_timestamp "
        f"FROM {table_name}) AS fare_tip_metrics"
    )
    row = jdbc_reader(query).collect()[0]
    return {field: str(row[field]) for field in row.asDict()}


def pending_publish_run_id():
    query = (
        "(SELECT publish_run_id FROM audit.publish_run_history "
        "WHERE target_table = 'ml.gold_fare_tip_features' AND status = 'started' "
        "ORDER BY publish_run_id DESC LIMIT 1) AS pending_publish"
    )
    rows = jdbc_reader(query).collect()
    if not rows:
        raise RuntimeError("No pending fare/tip PostgreSQL publish run found for validation.")
    return rows[0]["publish_run_id"]


def write_validation_results(rows):
    (
        spark.createDataFrame(rows)
        .withColumn("checked_at", current_timestamp())
        .write.format("jdbc")
        .option("url", WAREHOUSE_JDBC_URL)
        .option("dbtable", VALIDATION_TABLE)
        .option("user", WAREHOUSE_POSTGRES_USER)
        .option("password", WAREHOUSE_POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def main():
    if not WAREHOUSE_POSTGRES_USER or not WAREHOUSE_POSTGRES_PASSWORD:
        raise ValueError("Warehouse PostgreSQL credentials must be provided through environment variables.")

    source_metrics = fare_tip_metrics(spark.read.parquet(GOLD_FARE_TIP_FEATURES_PATH))
    target_metrics = postgres_fare_tip_metrics(SERVING_TABLE)
    logger.info("MinIO Gold fare/tip metrics: %s", source_metrics)
    logger.info("PostgreSQL fare/tip serving metrics: %s", target_metrics)

    publish_run_id = pending_publish_run_id()
    rows = []
    for check_name, expected_value in source_metrics.items():
        actual_value = target_metrics[check_name]
        rows.append(
            Row(
                publish_run_id=publish_run_id,
                check_name=check_name,
                expected_value=expected_value,
                actual_value=actual_value,
                status="pass" if expected_value == actual_value else "fail",
            )
        )

    write_validation_results(rows)
    failed = [row for row in rows if row.status == "fail"]
    if failed:
        raise RuntimeError(f"Fare/tip validation failed with {len(failed)} source-target mismatches.")

    logger.info("PostgreSQL fare/tip validation passed with %s checks", len(rows))


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
