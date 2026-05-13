import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    current_timestamp,
    date_format,
    date_trunc,
    from_json,
    lit,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
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

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")

BRONZE_YELLOW_PATH = os.getenv("BRONZE_YELLOW_PATH", "s3a://bronze/yellow_taxi/")
BRONZE_GREEN_PATH = os.getenv("BRONZE_GREEN_PATH", "s3a://bronze/green_taxi/")
BRONZE_WEATHER_PATH = os.getenv("BRONZE_WEATHER_PATH", "s3a://bronze/weather/")
SILVER_TAXI_WEATHER_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_PATH", "s3a://silver/taxi_weather_trips/"
)
SILVER_WEATHER_PATH = os.getenv("SILVER_WEATHER_PATH", "s3a://silver/hourly_weather/")
ZONE_LOOKUP_PATH = os.getenv("ZONE_LOOKUP_PATH", "data/taxi_zone_lookup.csv")


spark = (
    SparkSession.builder.appName("MetroPulse_Silver_Taxi_Weather_Enrichment")
    .config("spark.sql.session.timeZone", NYC_TIMEZONE)
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=America/New_York")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=America/New_York")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


taxi_schema = StructType(
    [
        StructField("VendorID", LongType()),
        StructField("tpep_pickup_datetime", StringType()),
        StructField("tpep_dropoff_datetime", StringType()),
        StructField("lpep_pickup_datetime", StringType()),
        StructField("lpep_dropoff_datetime", StringType()),
        StructField("passenger_count", DoubleType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", DoubleType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", LongType()),
        StructField("DOLocationID", LongType()),
        StructField("payment_type", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("ehail_fee", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("trip_type", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("airport_fee", DoubleType()),
        StructField("pickup_datetime", StringType()),
        StructField("dropoff_datetime", StringType()),
        StructField("_taxi_type", StringType()),
        StructField("_source_file", StringType()),
        StructField("_ingestion_timestamp", StringType()),
    ]
)

weather_schema = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("location", StringType()),
        StructField("temperature_f", DoubleType()),
        StructField("humidity_percent", DoubleType()),
        StructField("precipitation_mm", DoubleType()),
        StructField("weather_code", IntegerType()),
        StructField("weather_description", StringType()),
        StructField("wind_speed_kmh", DoubleType()),
        StructField("wind_direction_deg", DoubleType()),
        StructField("cloud_cover_percent", DoubleType()),
        StructField("_ingestion_timestamp", StringType()),
        StructField("_source", StringType()),
    ]
)

zone_schema = StructType(
    [
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),
        StructField("Latitude", DoubleType()),
        StructField("Longitude", DoubleType()),
    ]
)


def ensure_bronze_columns(bronze_df):
    defaults = {
        "topic": None,
        "partition": None,
        "offset": None,
        "kafka_timestamp": None,
        "key": None,
        "ingestion_timestamp": None,
        "ingestion_date": None,
    }
    result = bronze_df
    for name, value in defaults.items():
        if name not in result.columns:
            result = result.withColumn(name, lit(value))
    return result


def read_bronze_taxi():
    logger.info("Reading Bronze taxi parquet")
    yellow = spark.read.parquet(BRONZE_YELLOW_PATH).withColumn("taxi_type_from_path", lit("yellow"))
    green = spark.read.parquet(BRONZE_GREEN_PATH).withColumn("taxi_type_from_path", lit("green"))
    return ensure_bronze_columns(yellow.unionByName(green, allowMissingColumns=True))


def read_bronze_weather():
    logger.info("Reading Bronze weather parquet")
    return ensure_bronze_columns(spark.read.parquet(BRONZE_WEATHER_PATH))


def parse_taxi_payload(bronze_df):
    parsed = bronze_df.withColumn("record", from_json(col("json_data"), taxi_schema))
    return parsed.select(
        col("topic"),
        col("partition").cast("int").alias("partition"),
        col("offset").cast("long").alias("offset"),
        col("kafka_timestamp"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        coalesce(col("record._taxi_type"), col("taxi_type"), col("taxi_type_from_path")).alias("taxi_type"),
        col("record._source_file").alias("source_file"),
        col("record.VendorID").cast("long").alias("vendor_id"),
        coalesce(
            col("record.tpep_pickup_datetime"),
            col("record.lpep_pickup_datetime"),
            col("record.pickup_datetime"),
        ).alias("pickup_datetime_raw"),
        coalesce(
            col("record.tpep_dropoff_datetime"),
            col("record.lpep_dropoff_datetime"),
            col("record.dropoff_datetime"),
        ).alias("dropoff_datetime_raw"),
        col("record.passenger_count").cast("double").alias("passenger_count"),
        col("record.trip_distance").cast("double").alias("trip_distance"),
        col("record.RatecodeID").cast("int").alias("ratecode_id"),
        col("record.store_and_fwd_flag").alias("store_and_fwd_flag"),
        col("record.PULocationID").cast("int").alias("pu_location_id"),
        col("record.DOLocationID").cast("int").alias("do_location_id"),
        col("record.payment_type").cast("int").alias("payment_type"),
        col("record.fare_amount").cast("double").alias("fare_amount"),
        col("record.extra").cast("double").alias("extra"),
        col("record.mta_tax").cast("double").alias("mta_tax"),
        col("record.tip_amount").cast("double").alias("tip_amount"),
        col("record.tolls_amount").cast("double").alias("tolls_amount"),
        col("record.ehail_fee").cast("double").alias("ehail_fee"),
        col("record.improvement_surcharge").cast("double").alias("improvement_surcharge"),
        col("record.total_amount").cast("double").alias("total_amount"),
        col("record.trip_type").cast("int").alias("trip_type"),
        col("record.congestion_surcharge").cast("double").alias("congestion_surcharge"),
        col("record.airport_fee").cast("double").alias("airport_fee"),
    )


def standardize_taxi(taxi_df):
    return (
        taxi_df.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime_raw")))
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime_raw")))
        .withColumn("pickup_hour", date_trunc("hour", col("pickup_datetime")))
        .withColumn("pickup_date", to_date(col("pickup_datetime")))
        .withColumn("pickup_year_month", date_format(col("pickup_datetime"), "yyyy-MM"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("pickup_datetime_raw", "dropoff_datetime_raw")
        .dropna(subset=["pickup_datetime", "pickup_hour", "taxi_type", "pu_location_id"])
        .dropDuplicates(["topic", "partition", "offset"])
    )


def parse_weather_payload(bronze_df):
    parsed = bronze_df.withColumn("record", from_json(col("json_data"), weather_schema))
    return parsed.select(
        col("topic"),
        col("partition").cast("int").alias("partition"),
        col("offset").cast("long").alias("offset"),
        col("kafka_timestamp"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        col("record.timestamp").alias("weather_timestamp_raw"),
        col("record.latitude").alias("weather_latitude"),
        col("record.longitude").alias("weather_longitude"),
        col("record.location").alias("weather_location"),
        col("record.temperature_f"),
        col("record.humidity_percent"),
        col("record.precipitation_mm"),
        col("record.weather_code"),
        col("record.weather_description"),
        col("record.wind_speed_kmh"),
        col("record.wind_direction_deg"),
        col("record.cloud_cover_percent"),
        col("record._source").alias("weather_source"),
    )


def standardize_weather(weather_df):
    return (
        weather_df.withColumn("weather_timestamp", to_timestamp(col("weather_timestamp_raw")))
        .withColumn("pickup_hour", date_trunc("hour", col("weather_timestamp")))
        .withColumn("weather_date", to_date(col("weather_timestamp")))
        .withColumn("weather_year_month", date_format(col("weather_timestamp"), "yyyy-MM"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("weather_timestamp_raw")
        .dropna(subset=["pickup_hour"])
        .dropDuplicates(["topic", "partition", "offset"])
        .dropDuplicates(["pickup_hour"])
    )


def read_zone_lookup():
    zones = spark.read.option("header", "true").schema(zone_schema).csv(ZONE_LOOKUP_PATH)
    return zones.select(
        col("LocationID").alias("location_id"),
        col("Borough").alias("zone_borough"),
        col("Zone").alias("zone_name"),
        col("Latitude").alias("zone_lat"),
        col("Longitude").alias("zone_lon"),
    )


def enrich_zones(taxi_df, zones_df):
    pickup_zones = zones_df.select(
        col("location_id").alias("pu_location_id"),
        col("zone_borough").alias("pickup_zone_borough"),
        col("zone_name").alias("pickup_zone_name"),
        col("zone_lat").alias("pickup_zone_lat"),
        col("zone_lon").alias("pickup_zone_lon"),
    )
    dropoff_zones = zones_df.select(
        col("location_id").alias("do_location_id"),
        col("zone_borough").alias("dropoff_zone_borough"),
        col("zone_name").alias("dropoff_zone_name"),
        col("zone_lat").alias("dropoff_zone_lat"),
        col("zone_lon").alias("dropoff_zone_lon"),
    )

    return taxi_df.join(broadcast(pickup_zones), "pu_location_id", "left").join(
        broadcast(dropoff_zones), "do_location_id", "left"
    )


def enrich_weather(taxi_df, weather_df):
    weather_features = weather_df.select(
        "pickup_hour",
        "weather_timestamp",
        "weather_location",
        "temperature_f",
        "humidity_percent",
        "precipitation_mm",
        "weather_code",
        "weather_description",
        "wind_speed_kmh",
        "wind_direction_deg",
        "cloud_cover_percent",
    )
    return taxi_df.join(broadcast(weather_features), "pickup_hour", "left")


def write_outputs(weather_df, taxi_weather_df):
    logger.info("Writing Silver hourly weather")
    (
        weather_df.write.mode("overwrite")
        .format("parquet")
        .partitionBy("weather_year_month")
        .save(SILVER_WEATHER_PATH)
    )

    logger.info("Writing Silver taxi-weather trips")
    (
        taxi_weather_df.repartition("pickup_year_month", "taxi_type")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("pickup_year_month", "taxi_type")
        .save(SILVER_TAXI_WEATHER_PATH)
    )


def main():
    bronze_taxi = read_bronze_taxi()
    bronze_weather = read_bronze_weather()

    taxi = standardize_taxi(parse_taxi_payload(bronze_taxi))
    weather = standardize_weather(parse_weather_payload(bronze_weather))
    zones = read_zone_lookup()

    taxi_with_zones = enrich_zones(taxi, zones)
    taxi_weather = enrich_weather(taxi_with_zones, weather)

    write_outputs(weather, taxi_weather)
    logger.info("Silver enrichment completed")


if __name__ == "__main__":
    main()
