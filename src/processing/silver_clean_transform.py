import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
)
from pyspark.sql.types import ByteType, DecimalType, ShortType


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
SILVER_TAXI_WEATHER_CLEAN_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_CLEAN_PATH", "s3a://silver/taxi_weather_trips_clean/"
)


spark = (
    SparkSession.builder.appName("MetroPulse_Silver_Clean_Transform")
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


def apply_cleaning_rules(df):
    money_zero = lit("0.00").cast(DecimalType(12, 2))

    critical_columns = [
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

    result = df.dropna(subset=critical_columns)

    return (
        result.withColumn("is_passenger_count_missing", col("passenger_count").isNull())
        .withColumn("is_ratecode_id_missing", col("ratecode_id").isNull())
        .withColumn("is_payment_type_missing", col("payment_type").isNull())
        .withColumn("is_congestion_surcharge_missing", col("congestion_surcharge").isNull())
        .withColumn("is_airport_fee_missing", col("airport_fee").isNull())
        .withColumn(
            "passenger_count_clean",
            when(col("passenger_count").isNull(), lit(1).cast(ShortType())).otherwise(
                col("passenger_count").cast(ShortType())
            ),
        )
        .withColumn(
            "ratecode_id_clean",
            when(col("ratecode_id").isNull(), lit(99).cast(ByteType())).otherwise(
                col("ratecode_id").cast(ByteType())
            ),
        )
        .withColumn(
            "payment_type_clean",
            when(col("payment_type").isNull(), lit(0).cast(ByteType())).otherwise(
                col("payment_type").cast(ByteType())
            ),
        )
        .withColumn(
            "congestion_surcharge_clean",
            when(col("congestion_surcharge").isNull(), money_zero).otherwise(
                col("congestion_surcharge").cast(DecimalType(12, 2))
            ),
        )
        .withColumn(
            "airport_fee_clean",
            when(col("airport_fee").isNull(), money_zero).otherwise(
                col("airport_fee").cast(DecimalType(12, 2))
            ),
        )
        .withColumn(
            "is_gold_candidate",
            (~col("is_outlier_trip"))
            & col("is_valid_distance")
            & col("is_valid_fare")
            & col("is_valid_total_amount"),
        )
        .withColumn("clean_processed_timestamp", current_timestamp())
    )


def write_clean(clean_df):
    logger.info("Writing Silver clean taxi-weather trips to %s", SILVER_TAXI_WEATHER_CLEAN_PATH)
    (
        clean_df.repartition("pickup_year_month")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("pickup_year_month")
        .save(SILVER_TAXI_WEATHER_CLEAN_PATH)
    )


def main():
    logger.info("Reading Silver Core from %s", SILVER_TAXI_WEATHER_CORE_PATH)
    core = spark.read.parquet(SILVER_TAXI_WEATHER_CORE_PATH)

    clean = apply_cleaning_rules(core)
    write_clean(clean)

    logger.info("Silver clean transform completed")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
