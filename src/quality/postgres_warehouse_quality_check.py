import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import count, countDistinct, current_timestamp, max as spark_max, min as spark_min, sum as spark_sum


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
GOLD_DEMAND_FEATURES_PATH = os.getenv(
    "GOLD_DEMAND_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_demand_features/"
)

WAREHOUSE_JDBC_URL = os.getenv(
    "WAREHOUSE_JDBC_URL", "jdbc:postgresql://warehouse-postgres:5432/metropulse_dw"
)
WAREHOUSE_POSTGRES_USER = os.getenv("WAREHOUSE_POSTGRES_USER")
WAREHOUSE_POSTGRES_PASSWORD = os.getenv("WAREHOUSE_POSTGRES_PASSWORD")

SERVING_TABLE = "ml.gold_demand_features"
VALIDATION_TABLE = "audit.validation_results"


spark = (
    SparkSession.builder.appName("MetroPulse_Postgres_Warehouse_Quality_Check")
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


def demand_metrics(df):
    row = df.agg(
        count("*").alias("row_count"),
        spark_sum("demand").alias("total_demand"),
        countDistinct("pickup_year_month").alias("pickup_year_month_count"),
        spark_min("pickup_hour").alias("min_pickup_hour"),
        spark_max("pickup_hour").alias("max_pickup_hour"),
        spark_min("gold_processed_timestamp").alias("min_gold_processed_timestamp"),
        spark_max("gold_processed_timestamp").alias("max_gold_processed_timestamp"),
    ).collect()[0]
    return {field: str(row[field]) for field in row.asDict()}


def pending_publish_run_id():
    query = (
        "(SELECT publish_run_id FROM audit.publish_run_history "
        "WHERE target_table = 'ml.gold_demand_features' AND status = 'started' "
        "ORDER BY publish_run_id DESC LIMIT 1) AS pending_publish"
    )
    rows = jdbc_reader(query).collect()
    if not rows:
        raise RuntimeError("No pending PostgreSQL publish run found for validation.")
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

    source = spark.read.parquet(GOLD_DEMAND_FEATURES_PATH)
    target = jdbc_reader(SERVING_TABLE)
    source_metrics = demand_metrics(source)
    target_metrics = demand_metrics(target)
    logger.info("MinIO Gold metrics: %s", source_metrics)
    logger.info("PostgreSQL serving metrics: %s", target_metrics)

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
        raise RuntimeError(f"Warehouse validation failed with {len(failed)} source-target mismatches.")

    logger.info("PostgreSQL warehouse validation passed with %s checks", len(rows))


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
