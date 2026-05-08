from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, current_timestamp
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

logger.info(f"Initializing Spark Session for MetroPulse Bronze Layer...")
logger.info(f"MinIO Endpoint: {minio_endpoint}")

spark = SparkSession.builder \
    .appName("MetroPulse_Taxi_Weather_Bronze") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC -Dcom.sun.jndi.ldap.connect.pool=false") \
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC -Dcom.sun.jndi.ldap.connect.pool=false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
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
        topic_name: Kafka topic to subscribe to (yellow_taxi_stream or green_taxi_stream)
        bucket_path: S3A path to write data
        checkpoint_path: Checkpoint location for fault tolerance
    """
    logger.info(f"Setting up Taxi stream sink for topic: {topic_name}")
    
    # Read from Kafka (use internal port 29092 inside Docker)
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Bronze layer: keep data as raw JSON string
    # Schema validation will be done in Silver layer
    raw_df = df.selectExpr("CAST(value AS STRING) as json_data") \
               .withColumn("taxi_type", lit(topic_name.split('_')[0])) \
               .withColumn("ingestion_timestamp", current_timestamp())
    
    logger.info(f"  → Writing to: {bucket_path}")
    logger.info(f"  → Checkpoint: {checkpoint_path}")
    
    # Write to MinIO
    return raw_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", bucket_path) \
        .outputMode("append") \
        .start()


def create_weather_sink(bucket_path, checkpoint_path):
    """
    Create a streaming sink for Kafka weather topic to MinIO
    
    Args:
        bucket_path: S3A path to write data
        checkpoint_path: Checkpoint location for fault tolerance
    """
    logger.info(f"Setting up Weather stream sink")
    
    # Read from Kafka (use internal port 29092 inside Docker)
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "weather_stream") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Bronze layer: keep data as raw JSON string
    raw_df = df.selectExpr("CAST(value AS STRING) as json_data") \
               .withColumn("data_type", lit("weather")) \
               .withColumn("ingestion_timestamp", current_timestamp())
    
    logger.info(f"  → Writing to: {bucket_path}")
    logger.info(f"  → Checkpoint: {checkpoint_path}")
    
    # Write to MinIO
    return raw_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", bucket_path) \
        .outputMode("append") \
        .start()


if __name__ == "__main__":
    try:
        logger.info("Starting Bronze Layer Ingestion")
                
        # Run parallel streams for taxi types + weather
        logger.info("\nInitializing streams...\n")
        
        query_yellow = create_taxi_sink(
            "yellow_taxi_stream",
            "s3a://bronze/yellow_taxi/",
            "s3a://bronze/checkpoints/yellow/"
        )
        
        query_green = create_taxi_sink(
            "green_taxi_stream",
            "s3a://bronze/green_taxi/",
            "s3a://bronze/checkpoints/green/"
        )
        
        query_weather = create_weather_sink(
            "s3a://bronze/weather/",
            "s3a://bronze/checkpoints/weather/"
        )
        
        logger.info("All streams initialized successfully!")
        logger.info("\nMonitoring streams (press Ctrl+C to stop)...\n")
        
        # Wait for any stream to terminate
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
