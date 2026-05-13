from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, lit
import os
import sys
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set Python environment for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

load_dotenv()

# Get MinIO credentials from environment variables
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
minio_access_key = os.getenv('MINIO_ACCESS_KEY') 
minio_secret_key = os.getenv('MINIO_SECRET_KEY')  
yellow_taxi_topic = os.getenv('YELLOW_TAXI_TOPIC', 'nyc_taxi_yellow')
green_taxi_topic = os.getenv('GREEN_TAXI_TOPIC', 'nyc_taxi_green')
max_offsets_per_trigger = os.getenv('KAFKA_MAX_OFFSETS_PER_TRIGGER', '1000000')
bronze_trigger_available_now = os.getenv('BRONZE_TRIGGER_AVAILABLE_NOW', 'true').strip().lower() == 'true'
yellow_checkpoint_path = os.getenv('YELLOW_CHECKPOINT_PATH', 's3a://bronze/checkpoints/yellow_batched/')
green_checkpoint_path = os.getenv('GREEN_CHECKPOINT_PATH', 's3a://bronze/checkpoints/green/')
weather_checkpoint_path = os.getenv('WEATHER_CHECKPOINT_PATH', 's3a://bronze/checkpoints/weather/')

logger.info(f"Initializing Spark Session for MetroPulse Bronze Layer...")
logger.info(f"MinIO Endpoint: {minio_endpoint}")
logger.info(f"Bronze availableNow trigger: {bronze_trigger_available_now}")

spark = SparkSession.builder \
    .appName("MetroPulse_Taxi_Weather_Bronze") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC -Dcom.sun.jndi.ldap.connect.pool=false") \
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC -Dcom.sun.jndi.ldap.connect.pool=false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

logger.info("✓ Spark Session initialized successfully")

def create_taxi_sink(topic_name, bucket_path, checkpoint_path):
    """
    Create a streaming sink for Kafka taxi topic to MinIO
    
    Args:
        topic_name: Kafka topic to subscribe to
        bucket_path: S3A path to write data
        checkpoint_path: Checkpoint location for fault tolerance
    """
    logger.info(f"Setting up Taxi stream sink for topic: {topic_name}")
    
    # Read from Kafka (use internal port 29092 inside Docker)
    reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest")

    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)

    df = reader.load()
    
    # Bronze layer: keep raw Kafka payload and Kafka metadata.
    # Business schema validation and enrichment belong to Silver.
    ingestion_timestamp = current_timestamp()
    taxi_type = "yellow" if "yellow" in topic_name else "green"
    raw_df = df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("timestampType").alias("kafka_timestamp_type"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("json_data"),
    ).withColumn("taxi_type", lit(taxi_type)) \
     .withColumn("ingestion_timestamp", ingestion_timestamp) \
     .withColumn("ingestion_date", date_format(ingestion_timestamp, "yyyy-MM-dd"))
    
    logger.info(f"  → Writing to: {bucket_path}")
    logger.info(f"  → Checkpoint: {checkpoint_path}")
    
    # Write to MinIO
    writer = raw_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", bucket_path) \
        .partitionBy("ingestion_date") \
        .outputMode("append")
    if bronze_trigger_available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()


def create_weather_sink(bucket_path, checkpoint_path):
    """
    Create a streaming sink for Kafka weather topic to MinIO
    
    Args:
        bucket_path: S3A path to write data
        checkpoint_path: Checkpoint location for fault tolerance
    """
    logger.info(f"Setting up Weather stream sink")
    
    # Read from Kafka (use internal port 29092 inside Docker)
    reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "weather_stream") \
        .option("startingOffsets", "earliest")

    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)

    df = reader.load()
    
    # Bronze layer: keep raw Kafka payload and Kafka metadata.
    ingestion_timestamp = current_timestamp()
    raw_df = df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("timestampType").alias("kafka_timestamp_type"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("json_data"),
    ).withColumn("data_type", lit("weather")) \
     .withColumn("ingestion_timestamp", ingestion_timestamp) \
     .withColumn("ingestion_date", date_format(ingestion_timestamp, "yyyy-MM-dd"))
    
    logger.info(f"  → Writing to: {bucket_path}")
    logger.info(f"  → Checkpoint: {checkpoint_path}")
    
    # Write to MinIO
    writer = raw_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", bucket_path) \
        .partitionBy("ingestion_date") \
        .outputMode("append")
    if bronze_trigger_available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()


if __name__ == "__main__":
    try:
        logger.info("Starting Bronze Layer Ingestion")
                
        # Run parallel streams for taxi types + weather
        logger.info("\nInitializing streams...\n")
        
        query_yellow = create_taxi_sink(
            yellow_taxi_topic,
            "s3a://bronze/yellow_taxi/",
            yellow_checkpoint_path
        )
        
        query_green = create_taxi_sink(
            green_taxi_topic,
            "s3a://bronze/green_taxi/",
            green_checkpoint_path
        )
        
        query_weather = create_weather_sink(
            "s3a://bronze/weather/",
            weather_checkpoint_path
        )
        
        logger.info("All streams initialized successfully!")
        
        if bronze_trigger_available_now:
            logger.info("\nDraining available Kafka offsets, then stopping automatically...\n")
            for query in [query_yellow, query_green, query_weather]:
                query.awaitTermination()
        else:
            logger.info("\nMonitoring streams (press Ctrl+C to stop)...\n")
            spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        logger.info("\nStopping streams...")
        for query in spark.streams.active:
            query.stop()
        logger.info("All streams stopped")
        
    except Exception as e:
        logger.error(f"Error in Bronze ingestion: {e}")
        for query in spark.streams.active:
            query.stop()
        raise
        
    finally:
        logger.info("Bronze ingestion completed")
