import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import count, current_timestamp, max as spark_max, min as spark_min, sum as spark_sum


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

WAREHOUSE_JDBC_URL = os.getenv(
    "WAREHOUSE_JDBC_URL", "jdbc:postgresql://warehouse-postgres:5432/metropulse_dw"
)
WAREHOUSE_POSTGRES_USER = os.getenv("WAREHOUSE_POSTGRES_USER")
WAREHOUSE_POSTGRES_PASSWORD = os.getenv("WAREHOUSE_POSTGRES_PASSWORD")
VALIDATION_TABLE = "audit.validation_results"

DATASETS = [
    (
        "mart.dashboard_hourly_demand_kpi",
        os.getenv("DASHBOARD_HOURLY_DEMAND_KPI_PATH", f"{GOLD_ROOT_PATH}/dashboard_hourly_demand_kpi/"),
        "total_demand",
    ),
    (
        "mart.dashboard_zone_summary",
        os.getenv("DASHBOARD_ZONE_SUMMARY_PATH", f"{GOLD_ROOT_PATH}/dashboard_zone_summary/"),
        "total_demand",
    ),
    (
        "mart.dashboard_payment_tip_summary",
        os.getenv(
            "DASHBOARD_PAYMENT_TIP_SUMMARY_PATH", f"{GOLD_ROOT_PATH}/dashboard_payment_tip_summary/"
        ),
        "trip_count",
    ),
]

spark = (
    SparkSession.builder.appName("MetroPulse_Postgres_Dashboard_Quality_Check")
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


def mart_metrics(df, measure_column):
    row = df.agg(
        count("*").alias("row_count"),
        spark_sum(measure_column).alias(f"total_{measure_column}"),
        spark_min("source_gold_processed_timestamp").alias("min_source_gold_processed_timestamp"),
        spark_max("source_gold_processed_timestamp").alias("max_source_gold_processed_timestamp"),
        spark_min("dashboard_processed_timestamp").alias("min_dashboard_processed_timestamp"),
        spark_max("dashboard_processed_timestamp").alias("max_dashboard_processed_timestamp"),
    ).collect()[0]
    return {field: str(row[field]) for field in row.asDict()}


def pending_publish_run_id(table_name):
    query = (
        "(SELECT publish_run_id FROM audit.publish_run_history "
        f"WHERE target_table = '{table_name}' AND status = 'started' "
        "ORDER BY publish_run_id DESC LIMIT 1) AS pending_publish"
    )
    rows = jdbc_reader(query).collect()
    if not rows:
        raise RuntimeError(f"No pending PostgreSQL publish run found for {table_name}.")
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

    validation_rows = []
    for target_table, source_path, measure_column in DATASETS:
        source_metrics = mart_metrics(spark.read.parquet(source_path), measure_column)
        target_metrics = mart_metrics(jdbc_reader(target_table), measure_column)
        logger.info("%s MinIO metrics: %s", target_table, source_metrics)
        logger.info("%s PostgreSQL metrics: %s", target_table, target_metrics)
        publish_run_id = pending_publish_run_id(target_table)

        for check_name, expected_value in source_metrics.items():
            actual_value = target_metrics[check_name]
            validation_rows.append(
                Row(
                    publish_run_id=publish_run_id,
                    check_name=check_name,
                    expected_value=expected_value,
                    actual_value=actual_value,
                    status="pass" if expected_value == actual_value else "fail",
                )
            )

    write_validation_results(validation_rows)
    failed = [row for row in validation_rows if row.status == "fail"]
    if failed:
        raise RuntimeError(f"Dashboard validation failed with {len(failed)} source-target mismatches.")
    logger.info("PostgreSQL dashboard validation passed with %s checks", len(validation_rows))


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
