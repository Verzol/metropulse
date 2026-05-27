import logging
import os
import sys
import csv

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    expr,
    first,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
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

GOLD_ROOT_PATH = os.getenv("GOLD_ROOT_PATH", "s3a://gold")
GOLD_DEMAND_FEATURES_PATH = os.getenv(
    "GOLD_DEMAND_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_demand_features/"
)
GOLD_FARE_TIP_FEATURES_PATH = os.getenv(
    "GOLD_FARE_TIP_FEATURES_PATH", f"{GOLD_ROOT_PATH}/gold_fare_tip_features/"
)

DASHBOARD_HOURLY_DEMAND_KPI_PATH = os.getenv(
    "DASHBOARD_HOURLY_DEMAND_KPI_PATH", f"{GOLD_ROOT_PATH}/dashboard_hourly_demand_kpi/"
)
DASHBOARD_ZONE_SUMMARY_PATH = os.getenv(
    "DASHBOARD_ZONE_SUMMARY_PATH", f"{GOLD_ROOT_PATH}/dashboard_zone_summary/"
)
DASHBOARD_PAYMENT_TIP_SUMMARY_PATH = os.getenv(
    "DASHBOARD_PAYMENT_TIP_SUMMARY_PATH",
    f"{GOLD_ROOT_PATH}/dashboard_payment_tip_summary/",
)

ZONE_LOOKUP_PATH = os.getenv("ZONE_LOOKUP_PATH", "/tmp/taxi_zone_lookup.csv")


spark = (
    SparkSession.builder.appName("MetroPulse_Gold_Dashboard_Marts")
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


zone_schema = StructType(
    [
        StructField("LocationID", IntegerType()),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),
        StructField("Latitude", DoubleType()),
        StructField("Longitude", DoubleType()),
    ]
)


def read_zone_lookup():
    logger.info("Reading taxi zone lookup from %s", ZONE_LOOKUP_PATH)
    if ZONE_LOOKUP_PATH.startswith("s3a://"):
        return (
            spark.read.option("header", "true")
            .schema(zone_schema)
            .csv(ZONE_LOOKUP_PATH)
            .select(
                col("LocationID").cast(ShortType()).alias("pu_location_id"),
                col("Borough").alias("pickup_borough"),
                col("Zone").alias("pickup_zone"),
                col("Latitude").alias("pickup_latitude"),
                col("Longitude").alias("pickup_longitude"),
            )
        )

    rows = []
    with open(ZONE_LOOKUP_PATH, newline="", encoding="utf-8") as zone_file:
        reader = csv.DictReader(zone_file)
        for row in reader:
            rows.append(
                (
                    int(row["LocationID"]),
                    row["Borough"],
                    row["Zone"],
                    float(row["Latitude"]) if row["Latitude"] else None,
                    float(row["Longitude"]) if row["Longitude"] else None,
                )
            )

    dashboard_zone_schema = StructType(
        [
            StructField("pu_location_id", ShortType()),
            StructField("pickup_borough", StringType()),
            StructField("pickup_zone", StringType()),
            StructField("pickup_latitude", DoubleType()),
            StructField("pickup_longitude", DoubleType()),
        ]
    )
    return spark.createDataFrame(rows, dashboard_zone_schema)


def build_hourly_demand_kpi(demand_df):
    return (
        demand_df.groupBy("pickup_hour", "hour", "day_of_week", "month", "pickup_year_month")
        .agg(
            spark_sum("demand").cast(IntegerType()).alias("total_demand"),
            countDistinct("pu_location_id").cast(IntegerType()).alias("active_zones"),
            avg("demand").alias("avg_demand_per_active_zone"),
            spark_max("demand").cast(IntegerType()).alias("max_zone_hour_demand"),
            avg("temperature_f").alias("avg_temperature_f"),
            avg("precipitation_mm").alias("avg_precipitation_mm"),
            spark_max("gold_processed_timestamp").alias("source_gold_processed_timestamp"),
        )
        .withColumn("dashboard_processed_timestamp", current_timestamp())
    )


def build_zone_summary(demand_df, zones_df):
    zone_summary = (
        demand_df.groupBy("pu_location_id")
        .agg(
            spark_sum("demand").cast(IntegerType()).alias("total_demand"),
            avg("demand").alias("avg_hourly_demand"),
            spark_max("demand").cast(IntegerType()).alias("max_hourly_demand"),
            count("*").cast(IntegerType()).alias("active_hours"),
            spark_min("pickup_hour").alias("first_pickup_hour"),
            spark_max("pickup_hour").alias("last_pickup_hour"),
            avg("temperature_f").alias("avg_temperature_f"),
            avg("precipitation_mm").alias("avg_precipitation_mm"),
            spark_max("gold_processed_timestamp").alias("source_gold_processed_timestamp"),
        )
        .join(zones_df, "pu_location_id", "left")
        .select(
            "pu_location_id",
            "pickup_borough",
            "pickup_zone",
            "pickup_latitude",
            "pickup_longitude",
            "total_demand",
            "avg_hourly_demand",
            "max_hourly_demand",
            "active_hours",
            "first_pickup_hour",
            "last_pickup_hour",
            "avg_temperature_f",
            "avg_precipitation_mm",
            "source_gold_processed_timestamp",
            current_timestamp().alias("dashboard_processed_timestamp"),
        )
    )

    return zone_summary


def build_payment_tip_summary(fare_tip_df):
    return (
        fare_tip_df.groupBy("pickup_year_month", "payment_type")
        .agg(
            count("*").cast(IntegerType()).alias("trip_count"),
            avg("fare_amount").alias("avg_fare_amount"),
            avg("tip_amount").alias("avg_tip_amount"),
            avg("tip_percent").alias("avg_tip_percent"),
            expr("percentile_approx(tip_percent, 0.5, 10000)").alias("median_tip_percent"),
            expr("percentile_approx(fare_amount, 0.5, 10000)").alias("median_fare_amount"),
            avg("trip_distance").alias("avg_trip_distance"),
            spark_min("fare_amount").alias("min_fare_amount"),
            spark_max("fare_amount").alias("max_fare_amount"),
            spark_min("tip_percent").alias("min_tip_percent"),
            spark_max("tip_percent").alias("max_tip_percent"),
            first("month", ignorenulls=True).alias("month"),
            spark_max("gold_processed_timestamp").alias("source_gold_processed_timestamp"),
        )
        .withColumn("dashboard_processed_timestamp", current_timestamp())
    )


def write_parquet(df, path, partition_columns=None):
    logger.info("Writing dashboard mart to %s", path)
    writer = df.write.mode("overwrite").format("parquet")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(path)


def main():
    logger.info("Reading Gold demand features from %s", GOLD_DEMAND_FEATURES_PATH)
    demand = spark.read.parquet(GOLD_DEMAND_FEATURES_PATH)

    logger.info("Reading Gold fare/tip features from %s", GOLD_FARE_TIP_FEATURES_PATH)
    fare_tip = spark.read.parquet(GOLD_FARE_TIP_FEATURES_PATH)

    zones = read_zone_lookup()

    hourly_demand_kpi = build_hourly_demand_kpi(demand)
    zone_summary = build_zone_summary(demand, zones)
    payment_tip_summary = build_payment_tip_summary(fare_tip)

    write_parquet(
        hourly_demand_kpi.repartition("pickup_year_month"),
        DASHBOARD_HOURLY_DEMAND_KPI_PATH,
        ["pickup_year_month"],
    )
    write_parquet(zone_summary.coalesce(1), DASHBOARD_ZONE_SUMMARY_PATH)
    write_parquet(
        payment_tip_summary.repartition("pickup_year_month"),
        DASHBOARD_PAYMENT_TIP_SUMMARY_PATH,
        ["pickup_year_month"],
    )

    logger.info("Gold dashboard marts completed")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
