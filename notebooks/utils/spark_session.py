import os
from urllib.parse import urlparse, urlunparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, sum as spark_sum, when

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - notebooks may run before dependencies are installed.
    load_dotenv = None


if load_dotenv:
    load_dotenv()


NYC_TIMEZONE = "America/New_York"
EXPECTED_SPARK_VERSION = "3.5.1"

BRONZE_YELLOW_PATH = os.getenv("BRONZE_YELLOW_PATH", "s3a://bronze/yellow_taxi/")
BRONZE_GREEN_PATH = os.getenv("BRONZE_GREEN_PATH", "s3a://bronze/green_taxi/")
BRONZE_WEATHER_PATH = os.getenv("BRONZE_WEATHER_PATH", "s3a://bronze/weather/")
SILVER_WEATHER_PATH = os.getenv("SILVER_WEATHER_PATH", "s3a://silver/hourly_weather/")
SILVER_TAXI_WEATHER_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_PATH", "s3a://silver/taxi_weather_trips/"
)
SILVER_CORE_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_CORE_PATH", "s3a://silver/taxi_weather_trips_core/"
)
SILVER_CLEAN_PATH = os.getenv(
    "SILVER_TAXI_WEATHER_CLEAN_PATH", "s3a://silver/taxi_weather_trips_clean/"
)
SILVER_QUALITY_PATH = os.getenv(
    "SILVER_QUALITY_REPORT_PATH", "s3a://silver/quality_reports/silver_core_quality/latest/"
)
GOLD_ROOT_PATH = os.getenv("GOLD_ROOT_PATH", "s3a://gold/")


def _resolve_minio_endpoint(endpoint):
    """Use Docker DNS inside containers, but fall back to localhost for local notebooks."""
    parsed = urlparse(endpoint)
    if parsed.hostname != "minio":
        return endpoint

    try:
        import socket

        socket.gethostbyname("minio")
        return endpoint
    except OSError:
        netloc = "localhost"
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        return urlunparse(parsed._replace(netloc=netloc))


def _validate_pyspark_version():
    current_version = pyspark.__version__
    if current_version != EXPECTED_SPARK_VERSION:
        raise RuntimeError(
            "MetroPulse notebooks require PySpark "
            f"{EXPECTED_SPARK_VERSION} to match Spark 3.5.1 and Hadoop/S3A connectors. "
            f"Current kernel has PySpark {current_version}. "
            "Recreate the notebook kernel with Python 3.11 and run `make install`."
        )


def get_spark(app_name="MetroPulse Notebook EDA"):
    """Create or return a SparkSession configured for MetroPulse MinIO notebook EDA."""
    _validate_pyspark_version()

    minio_endpoint = _resolve_minio_endpoint(os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", NYC_TIMEZONE)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "48"))
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


def show_schema(df):
    """Print the Spark schema for quick notebook inspection."""
    df.printSchema()


def safe_display(df, n=20, truncate=False):
    """Display a small Spark DataFrame preview without collecting the full dataset."""
    df.show(n, truncate=truncate)


def count_by_partition(df, partition_col):
    """Return row counts grouped by a partition column."""
    return df.groupBy(partition_col).agg(count(lit(1)).alias("row_count")).orderBy(col(partition_col))


def null_profile(df, columns):
    """Return null counts and null ratios for selected columns."""
    total_rows = df.count()
    null_counts = df.agg(
        *[
            spark_sum(when(col(column_name).isNull(), 1).otherwise(0)).alias(column_name)
            for column_name in columns
        ]
    ).collect()[0]

    rows = [
        (
            column_name,
            int(null_counts[column_name] or 0),
            (int(null_counts[column_name] or 0) / total_rows) if total_rows else 0.0,
        )
        for column_name in columns
    ]
    return df.sparkSession.createDataFrame(rows, ["column_name", "null_count", "null_ratio"])


def small_to_pandas(df, max_rows=1000):
    """
    Convert only small aggregated Spark results to Pandas.

    This helper is intentionally guarded for notebook charts and summaries. It raises an
    error when the input has more than max_rows rows.
    """
    limited_rows = df.limit(max_rows + 1).collect()
    row_count = len(limited_rows)
    if row_count > max_rows:
        raise ValueError(
            "small_to_pandas is only for small aggregated results; "
            f"the DataFrame exceeds max_rows={max_rows}."
        )
    return df.sparkSession.createDataFrame(limited_rows, schema=df.schema).toPandas()


def path_exists(spark, path):
    """Return True when a MinIO/S3A or filesystem path exists without reading the dataset."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = spark.sparkContext._jvm.java.net.URI(path)
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    return bool(fs.exists(hadoop_path))
