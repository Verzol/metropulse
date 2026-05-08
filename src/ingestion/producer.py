import os
import pandas as pd
from kafka import KafkaProducer
import json
import time
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

logger.info(f"Connecting to Kafka at: {KAFKA_SERVER}\n")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 8, 0),
    request_timeout_ms=30000,
    max_block_ms=60000,
    metadata_max_age_ms=300000,
    compression_type='gzip',
    batch_size=16384,
    linger_ms=10
)

# Load NYC zone lookup table
zone_lookup = {}
try:
    zone_df = pd.read_csv('data/taxi_zone_lookup.csv')
    # Create dict with LocationID as key
    for _, row in zone_df.iterrows():
        zone_lookup[int(row['LocationID'])] = {
            'Borough': row['Borough'],
            'Zone': row['Zone'],
            'Latitude': row['Latitude'],
            'Longitude': row['Longitude']
        }
    logger.info(f"✓ Loaded {len(zone_lookup)} NYC taxi zones\n")
except FileNotFoundError:
    logger.warning("Zone lookup file not found. Proceeding without zone names.\n")
except Exception as e:
    logger.warning(f"Error loading zone lookup: {e}. Proceeding without zone names.\n")


def get_zone_info(location_id):
    """Get zone info from lookup table"""
    if location_id in zone_lookup:
        zone_data = zone_lookup[location_id]
        return {
            "zone_borough": zone_data['Borough'],
            "zone_name": zone_data['Zone'],
            "zone_lat": zone_data['Latitude'],
            "zone_lon": zone_data['Longitude']
        }
    return {}


def get_topic_from_filename(file_name):
    """Get Kafka topic from filename"""
    if 'yellow' in file_name.lower():
        return 'yellow_taxi_stream', 'yellow'
    elif 'green' in file_name.lower():
        return 'green_taxi_stream', 'green'
    else:
        raise ValueError(f"Cannot determine taxi type from file: {file_name}")


def normalize_taxi_record(row, taxi_type):
    """
    Normalize yellow/green taxi record to common schema
    
    Yellow columns: tpep_pickup_datetime, tpep_dropoff_datetime
    Green columns: lpep_pickup_datetime, lpep_dropoff_datetime
    """
    message = {}
    
    # Standardize datetime columns
    if taxi_type == 'yellow':
        pickup_col = 'tpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime'
    else:  # green
        pickup_col = 'lpep_pickup_datetime'
        dropoff_col = 'lpep_dropoff_datetime'
    
    # Copy all columns
    for col in row.index:
        val = row[col]
        
        # Convert timestamps to ISO format
        if isinstance(val, pd.Timestamp):
            message[col] = val.isoformat()
        # Handle NaN values
        elif pd.isna(val):
            message[col] = None
        else:
            message[col] = val
    
    # Add standardized datetime columns
    if pickup_col in message:
        message['pickup_datetime'] = message[pickup_col]
    if dropoff_col in message:
        message['dropoff_datetime'] = message[dropoff_col]
    
    # Add zone info for pickup location
    if 'PULocationID' in message and message['PULocationID']:
        pu_zone = get_zone_info(int(message['PULocationID']))
        if pu_zone:
            message['pickup_zone_borough'] = pu_zone.get('zone_borough')
            message['pickup_zone_name'] = pu_zone.get('zone_name')
            message['pickup_zone_lat'] = pu_zone.get('zone_lat')
            message['pickup_zone_lon'] = pu_zone.get('zone_lon')
    
    # Add zone info for dropoff location
    if 'DOLocationID' in message and message['DOLocationID']:
        do_zone = get_zone_info(int(message['DOLocationID']))
        if do_zone:
            message['dropoff_zone_borough'] = do_zone.get('zone_borough')
            message['dropoff_zone_name'] = do_zone.get('zone_name')
            message['dropoff_zone_lat'] = do_zone.get('zone_lat')
            message['dropoff_zone_lon'] = do_zone.get('zone_lon')
    
    # Add metadata
    message['_taxi_type'] = taxi_type
    message['_ingestion_timestamp'] = pd.Timestamp.now().isoformat()
    message['_source_file'] = None  # Will be set by caller
    
    return message


def stream_single_file(file_path):
    """Stream entire parquet file to Kafka with normalized schema"""
    file_name = os.path.basename(file_path)
    topic, taxi_type = get_topic_from_filename(file_name)
    
    try:
        logger.info(f"Processing: {file_name} → {topic}")
        df = pd.read_parquet(file_path)
        total_records = len(df)
        
        logger.info(f"   Total records: {total_records:,} | Sending ALL to Kafka...")
        
        start_time = time.time()
        
        for index, row in df.iterrows():
            message = normalize_taxi_record(row, taxi_type)
            message['_source_file'] = file_name
            
            producer.send(topic, value=message)
            
            if (index + 1) % 50000 == 0:
                elapsed = time.time() - start_time
                rate = (index + 1) / elapsed
                pct = (index + 1) / total_records * 100
                logger.info(f"   [{file_name}] {index + 1:,}/{total_records:,} ({pct:.1f}%) - {rate:.0f} records/sec")

        producer.flush()
        
        elapsed = time.time() - start_time
        logger.info(f"Completed: {file_name}")
        logger.info(f"Sent: {total_records:,} records in {elapsed:.1f}s ({total_records/elapsed:.0f} records/sec)\n")
        
    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}\n")
        raise


def stream_all_taxis(directory_path):
    """Stream all parquet files to Kafka"""
    files = sorted([
        os.path.join(directory_path, f) 
        for f in os.listdir(directory_path) 
        if f.endswith('.parquet')
    ])
    
    if not files:
        logger.error(f"No .parquet files found in {directory_path}")
        return
    
    logger.info(f"Found {len(files)} files to process:\n")
    for f in files:
        logger.info(f"  - {os.path.basename(f)}")
    logger.info()
    
    total_start = time.time()
    
    # Process files sequentially
    for file_path in files:
        stream_single_file(file_path)
        time.sleep(1)
    
    total_elapsed = time.time() - total_start
    logger.info("ALL FILES COMPLETED!")
    logger.info(f"Total time: {total_elapsed/60:.1f} minutes")


if __name__ == "__main__":
    try:
        logger.info("MetroPulse: NYC Taxi Data Producer")
        
        stream_all_taxis('data/raw/')
        
    except KeyboardInterrupt:
        logger.warning("\nStopped by user")
        logger.info("Flushing remaining messages...")
        producer.flush()
        
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        
    finally:
        producer.close()
        logger.info("Producer connection closed")