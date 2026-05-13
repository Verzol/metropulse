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
    first,
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

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BRONZE_YELLOW_PATH = os.getenv("BRONZE_YELLOW_PATH", "s3a://bronze/yellow_taxi/")
BRONZE_GREEN_PATH = os.getenv("BRONZE_GREEN_PATH", "s3a://bronze/green_taxi/")
BRONZE_WEATHER_PATH = os.getenv("BRONZE_WEATHER_PATH", "s3a://bronze/weather/")

SILVER_ROOT_PATH = os.getenv("SILVER_ROOT_PATH", "s3a://silver")
SILVER_WEATHER_PATH = os.getenv(
    "SILVER_WEATHER_PATH", f"{SILVER_ROOT_PATH}/hourly_weather/"
)
SILVER_TAXI_WEATHER_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_PATH", f"{SILVER_ROOT_PATH}/taxi_weather_trips/"
)
SILVER_PICKUP_START_DATE = os.getenv("SILVER_PICKUP_START_DATE", "2023-01-01")
SILVER_PICKUP_END_DATE = os.getenv("SILVER_PICKUP_END_DATE", "2025-01-01")


spark = (
    SparkSession.builder.appName("MetroPulse_Silver_Transform")
    .config("spark.sql.session.timeZone", NYC_TIMEZONE)
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
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


yellow_taxi_schema = StructType(
    [
        StructField("VendorID", LongType()),
        StructField("tpep_pickup_datetime", StringType()),
        StructField("tpep_dropoff_datetime", StringType()),
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
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("airport_fee", DoubleType()),
        StructField("pickup_datetime", StringType()),
        StructField("dropoff_datetime", StringType()),
        StructField("_taxi_type", StringType()),
        StructField("_source_file", StringType()),
        StructField("_ingestion_timestamp", StringType()),
    ]
)


green_taxi_schema = StructType(
    [
        StructField("VendorID", LongType()),
        StructField("lpep_pickup_datetime", StringType()),
        StructField("lpep_dropoff_datetime", StringType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("RatecodeID", DoubleType()),
        StructField("PULocationID", LongType()),
        StructField("DOLocationID", LongType()),
        StructField("passenger_count", DoubleType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("ehail_fee", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("payment_type", DoubleType()),
        StructField("trip_type", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
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


def ensure_bronze_columns(bronze_df):
    result = bronze_df
    defaults = {
        "topic": None,
        "partition": None,
        "offset": None,
        "kafka_timestamp": None,
        "key": None,
        "ingestion_timestamp": None,
        "ingestion_date": None,
    }
    for column_name, value in defaults.items():
        if column_name not in result.columns:
            result = result.withColumn(column_name, lit(value))
    return result


def parse_nyc_local_timestamp(timestamp_col):
    """Parse source event-time strings as America/New_York local timestamps."""
    return coalesce(
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(timestamp_col, "yyyy-MM-dd'T'HH:mm"),
        to_timestamp(timestamp_col, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        to_timestamp(timestamp_col, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(timestamp_col),
    )


def parse_yellow_taxi(bronze_df):
    parsed = ensure_bronze_columns(bronze_df).withColumn(
        "record", from_json(col("json_data"), yellow_taxi_schema)
    )
    return parsed.select(
        col("topic"),
        col("partition").cast("int").alias("partition"),
        col("offset").cast("long").alias("offset"),
        col("kafka_timestamp"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        lit("yellow").alias("taxi_type"),
        col("record._source_file").alias("source_file"),
        col("record.VendorID").cast("long").alias("vendor_id"),
        coalesce(col("record.tpep_pickup_datetime"), col("record.pickup_datetime")).alias(
            "pickup_datetime_raw"
        ),
        coalesce(col("record.tpep_dropoff_datetime"), col("record.dropoff_datetime")).alias(
            "dropoff_datetime_raw"
        ),
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
        lit(None).cast("double").alias("ehail_fee"),
        col("record.improvement_surcharge").cast("double").alias("improvement_surcharge"),
        col("record.total_amount").cast("double").alias("total_amount"),
        lit(None).cast("int").alias("trip_type"),
        col("record.congestion_surcharge").cast("double").alias("congestion_surcharge"),
        col("record.airport_fee").cast("double").alias("airport_fee"),
    )


def parse_green_taxi(bronze_df):
    parsed = ensure_bronze_columns(bronze_df).withColumn(
        "record", from_json(col("json_data"), green_taxi_schema)
    )
    return parsed.select(
        col("topic"),
        col("partition").cast("int").alias("partition"),
        col("offset").cast("long").alias("offset"),
        col("kafka_timestamp"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        lit("green").alias("taxi_type"),
        col("record._source_file").alias("source_file"),
        col("record.VendorID").cast("long").alias("vendor_id"),
        coalesce(col("record.lpep_pickup_datetime"), col("record.pickup_datetime")).alias(
            "pickup_datetime_raw"
        ),
        coalesce(col("record.lpep_dropoff_datetime"), col("record.dropoff_datetime")).alias(
            "dropoff_datetime_raw"
        ),
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
        lit(None).cast("double").alias("airport_fee"),
    )


def normalize_taxi(yellow_df, green_df):
    taxi = yellow_df.unionByName(green_df, allowMissingColumns=True)
    return (
        taxi.withColumn("source_pickup_datetime", col("pickup_datetime_raw"))
        .withColumn("source_dropoff_datetime", col("dropoff_datetime_raw"))
        .withColumn("pickup_datetime", parse_nyc_local_timestamp(col("pickup_datetime_raw")))
        .withColumn("dropoff_datetime", parse_nyc_local_timestamp(col("dropoff_datetime_raw")))
        .withColumn("pickup_hour", date_trunc("hour", col("pickup_datetime")))
        .withColumn("pickup_date", to_date(col("pickup_datetime")))
        .withColumn("pickup_year_month", date_format(col("pickup_datetime"), "yyyy-MM"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("pickup_datetime_raw", "dropoff_datetime_raw")
        .dropna(subset=["pickup_datetime", "pickup_hour", "taxi_type", "pu_location_id"])
        .filter(
            (col("pickup_date") >= lit(SILVER_PICKUP_START_DATE).cast("date"))
            & (col("pickup_date") < lit(SILVER_PICKUP_END_DATE).cast("date"))
        )
        .filter(
            col("dropoff_datetime").isNull()
            | (col("dropoff_datetime") >= col("pickup_datetime"))
        )
        .dropDuplicates(["topic", "partition", "offset"])
        .dropDuplicates(
            [
                "taxi_type",
                "source_file",
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "pu_location_id",
                "do_location_id",
                "trip_distance",
                "fare_amount",
                "total_amount",
            ]
        )
    )


def parse_weather(bronze_df):
    parsed = ensure_bronze_columns(bronze_df).withColumn(
        "record", from_json(col("json_data"), weather_schema)
    )
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
        col("record.temperature_f").cast("double").alias("temperature_f"),
        col("record.humidity_percent").cast("double").alias("humidity_percent"),
        col("record.precipitation_mm").cast("double").alias("precipitation_mm"),
        col("record.weather_code").cast("int").alias("weather_code"),
        col("record.weather_description").alias("weather_description"),
        col("record.wind_speed_kmh").cast("double").alias("wind_speed_kmh"),
        col("record.wind_direction_deg").cast("double").alias("wind_direction_deg"),
        col("record.cloud_cover_percent").cast("double").alias("cloud_cover_percent"),
        col("record._source").alias("weather_source"),
    )


def normalize_weather(weather_df):
    weather = (
        weather_df.withColumn("source_weather_timestamp", col("weather_timestamp_raw"))
        .withColumn("weather_timestamp", parse_nyc_local_timestamp(col("weather_timestamp_raw")))
        .withColumn("weather_hour", date_trunc("hour", col("weather_timestamp")))
        .withColumn("weather_date", to_date(col("weather_timestamp")))
        .withColumn("weather_year_month", date_format(col("weather_timestamp"), "yyyy-MM"))
        .withColumn("silver_processed_timestamp", current_timestamp())
        .drop("weather_timestamp_raw")
        .dropna(subset=["weather_hour"])
        .filter(
            (col("weather_date") >= lit(SILVER_PICKUP_START_DATE).cast("date"))
            & (col("weather_date") < lit(SILVER_PICKUP_END_DATE).cast("date"))
        )
        .dropDuplicates(["topic", "partition", "offset"])
    )

    # Keep one weather row per NYC local hour so taxi joins cannot multiply trip rows.
    return weather.groupBy("weather_hour").agg(
        first("weather_timestamp", ignorenulls=True).alias("weather_timestamp"),
        first("source_weather_timestamp", ignorenulls=True).alias("source_weather_timestamp"),
        first("weather_date", ignorenulls=True).alias("weather_date"),
        first("weather_year_month", ignorenulls=True).alias("weather_year_month"),
        first("weather_latitude", ignorenulls=True).alias("weather_latitude"),
        first("weather_longitude", ignorenulls=True).alias("weather_longitude"),
        first("weather_location", ignorenulls=True).alias("weather_location"),
        first("temperature_f", ignorenulls=True).alias("temperature_f"),
        first("humidity_percent", ignorenulls=True).alias("humidity_percent"),
        first("precipitation_mm", ignorenulls=True).alias("precipitation_mm"),
        first("weather_code", ignorenulls=True).alias("weather_code"),
        first("weather_description", ignorenulls=True).alias("weather_description"),
        first("wind_speed_kmh", ignorenulls=True).alias("wind_speed_kmh"),
        first("wind_direction_deg", ignorenulls=True).alias("wind_direction_deg"),
        first("cloud_cover_percent", ignorenulls=True).alias("cloud_cover_percent"),
        first("weather_source", ignorenulls=True).alias("weather_source"),
        first("silver_processed_timestamp", ignorenulls=True).alias(
            "silver_processed_timestamp"
        ),
    )


def join_taxi_with_hourly_weather(taxi_df, weather_df):
    weather_features = weather_df.select(
        "weather_hour",
        "weather_timestamp",
        "source_weather_timestamp",
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
    return (
        taxi_df.join(
            broadcast(weather_features),
            taxi_df["pickup_hour"] == weather_features["weather_hour"],
            "left",
        )
        .drop(weather_features["weather_hour"])
    )


def write_silver_outputs(weather_df, taxi_weather_df):
    logger.info("Writing Silver hourly weather to %s", SILVER_WEATHER_PATH)
    (
        weather_df.repartition("weather_year_month")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("weather_year_month")
        .save(SILVER_WEATHER_PATH)
    )

    logger.info("Writing Silver taxi-weather trips to %s", SILVER_TAXI_WEATHER_PATH)
    (
        taxi_weather_df.repartition("pickup_year_month", "taxi_type")
        .write.mode("overwrite")
        .format("parquet")
        .partitionBy("pickup_year_month", "taxi_type")
        .save(SILVER_TAXI_WEATHER_PATH)
    )


def main():
    logger.info("Starting Silver transform with timezone=%s", NYC_TIMEZONE)
    logger.info(
        "Filtering taxi pickup_datetime range: [%s, %s)",
        SILVER_PICKUP_START_DATE,
        SILVER_PICKUP_END_DATE,
    )
    logger.info("Reading Bronze Yellow taxi from %s", BRONZE_YELLOW_PATH)
    bronze_yellow = spark.read.parquet(BRONZE_YELLOW_PATH)

    logger.info("Reading Bronze Green taxi from %s", BRONZE_GREEN_PATH)
    bronze_green = spark.read.parquet(BRONZE_GREEN_PATH)

    logger.info("Reading Bronze weather from %s", BRONZE_WEATHER_PATH)
    bronze_weather = spark.read.parquet(BRONZE_WEATHER_PATH)

    yellow = parse_yellow_taxi(bronze_yellow)
    green = parse_green_taxi(bronze_green)
    taxi = normalize_taxi(yellow, green)
    weather = normalize_weather(parse_weather(bronze_weather))
    taxi_weather = join_taxi_with_hourly_weather(taxi, weather)

    write_silver_outputs(weather, taxi_weather)
    logger.info("Silver transform completed")


if __name__ == "__main__":
    main()
