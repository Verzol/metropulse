import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, max as spark_max, min as spark_min, sum as spark_sum, when


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
WAREHOUSE_FARE_TIP_JDBC_WRITE_PARTITIONS = int(
    os.getenv("WAREHOUSE_FARE_TIP_JDBC_WRITE_PARTITIONS", "2")
)
WAREHOUSE_JDBC_BATCH_SIZE = os.getenv("WAREHOUSE_JDBC_BATCH_SIZE", "5000")
WAREHOUSE_FARE_TIP_REUSE_STAGING = (
    os.getenv("WAREHOUSE_FARE_TIP_REUSE_STAGING", "false").lower() == "true"
)

STAGING_TABLE = "staging.gold_fare_tip_features_staging"

FARE_TIP_COLUMNS = [
    "fare_amount",
    "tip_amount",
    "tip_percent",
    "trip_distance",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "ratecode_id",
    "payment_type",
    "hour",
    "day_of_week",
    "month",
    "temperature_f",
    "precipitation_mm",
    "pickup_year_month",
    "gold_processed_timestamp",
]

spark = (
    SparkSession.builder.appName("MetroPulse_Publish_Gold_Fare_Tip_To_Postgres")
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


def main():
    if not WAREHOUSE_POSTGRES_USER or not WAREHOUSE_POSTGRES_PASSWORD:
        raise ValueError("Warehouse PostgreSQL credentials must be provided through environment variables.")

    logger.info("Reading Gold fare/tip features from %s", GOLD_FARE_TIP_FEATURES_PATH)
    source = spark.read.parquet(GOLD_FARE_TIP_FEATURES_PATH).select(*FARE_TIP_COLUMNS)
    source_metrics = fare_tip_metrics(source)
    logger.info("Fare/tip source publication metrics: %s", source_metrics)

    if WAREHOUSE_FARE_TIP_REUSE_STAGING:
        logger.info("Reusing existing staging table %s after an interrupted validation.", STAGING_TABLE)
    else:
        logger.info(
            "Writing staging table %s via JDBC using %s partitions",
            STAGING_TABLE,
            WAREHOUSE_FARE_TIP_JDBC_WRITE_PARTITIONS,
        )
        (
            source.coalesce(WAREHOUSE_FARE_TIP_JDBC_WRITE_PARTITIONS)
            .write.format("jdbc")
            .option("url", WAREHOUSE_JDBC_URL)
            .option("dbtable", STAGING_TABLE)
            .option("user", WAREHOUSE_POSTGRES_USER)
            .option("password", WAREHOUSE_POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", WAREHOUSE_JDBC_BATCH_SIZE)
            .mode("overwrite")
            .save()
        )

    staging_metrics = postgres_fare_tip_metrics(STAGING_TABLE)
    logger.info("Fare/tip staging publication metrics: %s", staging_metrics)
    if source_metrics != staging_metrics:
        raise RuntimeError(
            f"Fare/tip staging validation failed: source_metrics={source_metrics}, "
            f"staging_metrics={staging_metrics}"
        )

    logger.info("Fare/tip staging table matches the Gold source; ready for promotion.")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
