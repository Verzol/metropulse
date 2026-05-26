import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_format,
    date_trunc,
    dayofweek,
    hour,
    lit,
    month,
)
from pyspark.sql.types import (
    ByteType,
    DecimalType,
    FloatType,
    IntegerType,
    ShortType,
    TimestampType,
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

SILVER_TAXI_WEATHER_CORE_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_CORE_PATH", "s3a://silver/taxi_weather_trips_core/"
)

GOLD_ROOT_PATH = os.getenv("GOLD_ROOT_PATH", "s3a://gold")
GOLD_DEMAND_FEATURES_PATH = os.getenv(
    "GOLD_DEMAND_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_demand_features/"
)
GOLD_FARE_TIP_FEATURES_PATH = os.getenv(
    "GOLD_FARE_TIP_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_fare_tip_features/"
)

MIN_TIP_MODEL_FARE_AMOUNT = float(os.getenv("MIN_TIP_MODEL_FARE_AMOUNT", "2.5"))
MAX_FARE_MODEL_AMOUNT = float(os.getenv("MAX_FARE_MODEL_AMOUNT", "300.0"))
MAX_TIP_PERCENT = float(os.getenv("MAX_TIP_PERCENT", "100.0"))


spark = (
    SparkSession.builder.appName("MetroPulse_Gold_Transform")
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


def filter_gold_candidate_trips(df):
    return df.filter(
        (col("is_valid_distance") == lit(True))
        & (col("is_valid_fare") == lit(True))
        & (col("is_outlier_trip") == lit(False))
    )


def build_gold_demand_features(core_df):
    """Build hourly zone-level demand features for XGBoost demand prediction."""
    valid_trips = filter_gold_candidate_trips(core_df)

    demand_features = (
        valid_trips.withColumn("pickup_hour", date_trunc("hour", col("pickup_datetime")))
        .groupBy("pu_location_id", "pickup_hour")
        .agg(
            count("*").cast(IntegerType()).alias("demand"),
            avg("temperature_f").cast(FloatType()).alias("temperature_f"),
            avg("precipitation_mm").cast(FloatType()).alias("precipitation_mm"),
        )
        .select(
            col("pu_location_id").cast(ShortType()).alias("pu_location_id"),
            col("pickup_hour").cast(TimestampType()).alias("pickup_hour"),
            col("demand").cast(IntegerType()).alias("demand"),
            hour(col("pickup_hour")).cast(ByteType()).alias("hour"),
            dayofweek(col("pickup_hour")).cast(ByteType()).alias("day_of_week"),
            month(col("pickup_hour")).cast(ByteType()).alias("month"),
            col("temperature_f").cast(FloatType()).alias("temperature_f"),
            col("precipitation_mm").cast(FloatType()).alias("precipitation_mm"),
            date_format(col("pickup_hour"), "yyyy-MM").alias("pickup_year_month"),
            current_timestamp().alias("gold_processed_timestamp"),
        )
    )

    return demand_features


def build_gold_fare_tip_features(core_df):
    """Build trip-level fare and tip features for LightGBM extension modeling."""
    tip_percent_expr = (
        (col("tip_amount").cast(FloatType()) / col("fare_amount").cast(FloatType())) * lit(100.0)
    )

    valid_trips = filter_gold_candidate_trips(core_df).filter(
        (col("fare_amount") >= lit(MIN_TIP_MODEL_FARE_AMOUNT))
        & (col("fare_amount") <= lit(MAX_FARE_MODEL_AMOUNT))
        & (col("trip_distance") > lit(0))
        & (col("tip_amount") >= lit(0))
        & (tip_percent_expr <= lit(MAX_TIP_PERCENT))
    )

    fare_tip_features = valid_trips.select(
        col("fare_amount").cast(DecimalType(12, 2)).alias("fare_amount"),
        col("tip_amount").cast(DecimalType(12, 2)).alias("tip_amount"),
        tip_percent_expr.cast(FloatType()).alias("tip_percent"),
        col("trip_distance").cast(FloatType()).alias("trip_distance"),
        col("pu_location_id").cast(ShortType()).alias("pu_location_id"),
        col("do_location_id").cast(ShortType()).alias("do_location_id"),
        col("passenger_count").cast(ShortType()).alias("passenger_count"),
        col("ratecode_id").cast(ByteType()).alias("ratecode_id"),
        col("payment_type").cast(ByteType()).alias("payment_type"),
        hour(col("pickup_datetime")).cast(ByteType()).alias("hour"),
        dayofweek(col("pickup_datetime")).cast(ByteType()).alias("day_of_week"),
        month(col("pickup_datetime")).cast(ByteType()).alias("month"),
        col("temperature_f").cast(FloatType()).alias("temperature_f"),
        col("precipitation_mm").cast(FloatType()).alias("precipitation_mm"),
        col("pickup_year_month"),
        current_timestamp().alias("gold_processed_timestamp"),
    )

    return fare_tip_features


def write_gold_demand_features(demand_features_df):
    logger.info("Writing %s to %s", GOLD_DEMAND_FEATURES, GOLD_DEMAND_FEATURES_PATH)
    (
        demand_features_df.repartition("pickup_year_month")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("pickup_year_month")
        .save(GOLD_DEMAND_FEATURES_PATH)
    )


def write_gold_fare_tip_features(fare_tip_features_df):
    logger.info("Writing %s to %s", GOLD_FARE_TIP_FEATURES, GOLD_FARE_TIP_FEATURES_PATH)
    (
        fare_tip_features_df.repartition("pickup_year_month")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("pickup_year_month")
        .save(GOLD_FARE_TIP_FEATURES_PATH)
    )


def main():
    logger.info("Reading Silver Core from %s", SILVER_TAXI_WEATHER_CORE_PATH)
    core = spark.read.parquet(SILVER_TAXI_WEATHER_CORE_PATH)

    demand_features = build_gold_demand_features(core)
    fare_tip_features = build_gold_fare_tip_features(core)

    write_gold_demand_features(demand_features)
    write_gold_fare_tip_features(fare_tip_features)

    logger.info("Gold transform completed")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
