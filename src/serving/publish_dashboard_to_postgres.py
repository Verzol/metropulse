import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max as spark_max, min as spark_min, sum as spark_sum


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

DASHBOARD_HOURLY_DEMAND_KPI_PATH = os.getenv(
    "DASHBOARD_HOURLY_DEMAND_KPI_PATH", f"{GOLD_ROOT_PATH}/dashboard_hourly_demand_kpi/"
)
DASHBOARD_ZONE_SUMMARY_PATH = os.getenv(
    "DASHBOARD_ZONE_SUMMARY_PATH", f"{GOLD_ROOT_PATH}/dashboard_zone_summary/"
)
DASHBOARD_PAYMENT_TIP_SUMMARY_PATH = os.getenv(
    "DASHBOARD_PAYMENT_TIP_SUMMARY_PATH", f"{GOLD_ROOT_PATH}/dashboard_payment_tip_summary/"
)

WAREHOUSE_JDBC_URL = os.getenv(
    "WAREHOUSE_JDBC_URL", "jdbc:postgresql://warehouse-postgres:5432/metropulse_dw"
)
WAREHOUSE_POSTGRES_USER = os.getenv("WAREHOUSE_POSTGRES_USER")
WAREHOUSE_POSTGRES_PASSWORD = os.getenv("WAREHOUSE_POSTGRES_PASSWORD")
WAREHOUSE_JDBC_BATCH_SIZE = os.getenv("WAREHOUSE_JDBC_BATCH_SIZE", "5000")

DATASETS = [
    (
        "dashboard_hourly_demand_kpi",
        DASHBOARD_HOURLY_DEMAND_KPI_PATH,
        "staging.dashboard_hourly_demand_kpi_staging",
        "total_demand",
    ),
    (
        "dashboard_zone_summary",
        DASHBOARD_ZONE_SUMMARY_PATH,
        "staging.dashboard_zone_summary_staging",
        "total_demand",
    ),
    (
        "dashboard_payment_tip_summary",
        DASHBOARD_PAYMENT_TIP_SUMMARY_PATH,
        "staging.dashboard_payment_tip_summary_staging",
        "trip_count",
    ),
]

spark = (
    SparkSession.builder.appName("MetroPulse_Publish_Dashboard_Marts_To_Postgres")
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


def main():
    if not WAREHOUSE_POSTGRES_USER or not WAREHOUSE_POSTGRES_PASSWORD:
        raise ValueError("Warehouse PostgreSQL credentials must be provided through environment variables.")

    for dataset_name, source_path, staging_table, measure_column in DATASETS:
        logger.info("Reading dashboard mart %s from %s", dataset_name, source_path)
        source = spark.read.parquet(source_path)
        source_metrics = mart_metrics(source, measure_column)
        logger.info("%s source metrics: %s", dataset_name, source_metrics)

        (
            source.coalesce(1)
            .write.format("jdbc")
            .option("url", WAREHOUSE_JDBC_URL)
            .option("dbtable", staging_table)
            .option("user", WAREHOUSE_POSTGRES_USER)
            .option("password", WAREHOUSE_POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", WAREHOUSE_JDBC_BATCH_SIZE)
            .mode("overwrite")
            .save()
        )

        staging_metrics = mart_metrics(jdbc_reader(staging_table), measure_column)
        logger.info("%s staging metrics: %s", dataset_name, staging_metrics)
        if source_metrics != staging_metrics:
            raise RuntimeError(
                f"Staging validation failed for {dataset_name}: "
                f"source_metrics={source_metrics}, staging_metrics={staging_metrics}"
            )

    logger.info("All dashboard staging tables match Gold mart sources; ready for promotion.")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
