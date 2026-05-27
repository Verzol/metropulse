import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, max as spark_max, min as spark_min, sum as spark_sum


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
WAREHOUSE_JDBC_WRITE_PARTITIONS = int(os.getenv("WAREHOUSE_JDBC_WRITE_PARTITIONS", "4"))
WAREHOUSE_JDBC_BATCH_SIZE = os.getenv("WAREHOUSE_JDBC_BATCH_SIZE", "5000")

STAGING_TABLE = "staging.gold_demand_features_staging"

DEMAND_COLUMNS = [
    "pu_location_id",
    "pickup_hour",
    "demand",
    "hour",
    "day_of_week",
    "month",
    "temperature_f",
    "precipitation_mm",
    "pickup_year_month",
    "gold_processed_timestamp",
]


spark = (
    SparkSession.builder.appName("MetroPulse_Publish_Gold_Demand_To_Postgres")
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
    result = (
        df.agg(
            count("*").alias("row_count"),
            spark_sum("demand").alias("total_demand"),
            countDistinct("pickup_year_month").alias("pickup_year_month_count"),
            spark_min("pickup_hour").alias("min_pickup_hour"),
            spark_max("pickup_hour").alias("max_pickup_hour"),
            spark_min("gold_processed_timestamp").alias("min_gold_processed_timestamp"),
            spark_max("gold_processed_timestamp").alias("max_gold_processed_timestamp"),
        )
        .collect()[0]
        .asDict()
    )
    return {key: str(value) for key, value in result.items()}


def main():
    if not WAREHOUSE_POSTGRES_USER or not WAREHOUSE_POSTGRES_PASSWORD:
        raise ValueError("Warehouse PostgreSQL credentials must be provided through environment variables.")

    logger.info("Reading Gold demand features from %s", GOLD_DEMAND_FEATURES_PATH)
    source = spark.read.parquet(GOLD_DEMAND_FEATURES_PATH).select(*DEMAND_COLUMNS)
    source_metrics = demand_metrics(source)
    logger.info("Source publication metrics: %s", source_metrics)

    logger.info(
        "Writing staging table %s via JDBC using %s partitions",
        STAGING_TABLE,
        WAREHOUSE_JDBC_WRITE_PARTITIONS,
    )
    (
        source.coalesce(WAREHOUSE_JDBC_WRITE_PARTITIONS)
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

    staging_metrics = demand_metrics(jdbc_reader(STAGING_TABLE))
    logger.info("Staging publication metrics: %s", staging_metrics)

    if source_metrics != staging_metrics:
        raise RuntimeError(
            f"Staging validation failed: source_metrics={source_metrics}, "
            f"staging_metrics={staging_metrics}"
        )

    logger.info("Staging table is complete and matches the Gold source; ready for promotion.")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
