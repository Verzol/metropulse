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
from pyspark.sql.types import (
    ByteType,
    DateType,
    DecimalType,
    FloatType,
    ShortType,
    StringType,
    TimestampType,
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

SILVER_TAXI_WEATHER_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_PATH", "s3a://silver/taxi_weather_trips/"
)
SILVER_TAXI_WEATHER_CORE_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_CORE_PATH", "s3a://silver/taxi_weather_trips_core/"
)


spark = (
    SparkSession.builder.appName("MetroPulse_Silver_Taxi_Weather_Core")
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


def build_core_schema(df):
    """Create a compact row-level Silver fact table for Gold feature engineering."""
    money_type = DecimalType(12, 2)

    return df.select(
        col("taxi_type").cast(StringType()).alias("taxi_type"),
        col("vendor_id").cast(ByteType()).alias("vendor_id"),
        col("pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        col("dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        col("pickup_hour").cast(TimestampType()).alias("pickup_hour"),
        col("pickup_date").cast(DateType()).alias("pickup_date"),
        col("pickup_year_month").cast(StringType()).alias("pickup_year_month"),
        col("pu_location_id").cast(ShortType()).alias("pu_location_id"),
        col("do_location_id").cast(ShortType()).alias("do_location_id"),
        col("passenger_count").cast(ShortType()).alias("passenger_count"),
        col("trip_distance").cast(FloatType()).alias("trip_distance"),
        col("ratecode_id").cast(ByteType()).alias("ratecode_id"),
        col("payment_type").cast(ByteType()).alias("payment_type"),
        col("fare_amount").cast(money_type).alias("fare_amount"),
        col("extra").cast(money_type).alias("extra"),
        col("mta_tax").cast(money_type).alias("mta_tax"),
        col("tip_amount").cast(money_type).alias("tip_amount"),
        col("tolls_amount").cast(money_type).alias("tolls_amount"),
        col("improvement_surcharge").cast(money_type).alias("improvement_surcharge"),
        col("total_amount").cast(money_type).alias("total_amount"),
        col("congestion_surcharge").cast(money_type).alias("congestion_surcharge"),
        col("airport_fee").cast(money_type).alias("airport_fee"),
        col("temperature_f").cast(FloatType()).alias("temperature_f"),
        col("humidity_percent").cast(FloatType()).alias("humidity_percent"),
        col("precipitation_mm").cast(FloatType()).alias("precipitation_mm"),
        col("weather_code").cast(ShortType()).alias("weather_code"),
        col("wind_speed_kmh").cast(FloatType()).alias("wind_speed_kmh"),
        col("wind_direction_deg").cast(FloatType()).alias("wind_direction_deg"),
        col("cloud_cover_percent").cast(FloatType()).alias("cloud_cover_percent"),
        when((col("trip_distance") > 0) & (col("trip_distance") <= 100), lit(True))
        .otherwise(lit(False))
        .alias("is_valid_distance"),
        when((col("fare_amount") >= 0) & (col("fare_amount") <= 1000), lit(True))
        .otherwise(lit(False))
        .alias("is_valid_fare"),
        when((col("total_amount") >= 0) & (col("total_amount") <= 1500), lit(True))
        .otherwise(lit(False))
        .alias("is_valid_total_amount"),
        (
            (~((col("trip_distance") > 0) & (col("trip_distance") <= 100)))
            | (~((col("fare_amount") >= 0) & (col("fare_amount") <= 1000)))
            | (~((col("total_amount") >= 0) & (col("total_amount") <= 1500)))
        ).alias("is_outlier_trip"),
        current_timestamp().alias("core_processed_timestamp"),
    )


def write_core(core_df):
    logger.info("Writing Silver core taxi-weather trips to %s", SILVER_TAXI_WEATHER_CORE_PATH)
    (
        core_df.repartition("pickup_year_month")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("pickup_year_month")
        .save(SILVER_TAXI_WEATHER_CORE_PATH)
    )


def main():
    logger.info("Reading existing Silver taxi-weather trips from %s", SILVER_TAXI_WEATHER_PATH)
    taxi_weather = spark.read.parquet(SILVER_TAXI_WEATHER_PATH)

    core = build_core_schema(taxi_weather)
    write_core(core)

    logger.info("Silver core transform completed")


if __name__ == "__main__":
    main()
