import os
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
import json
import time
from dotenv import load_dotenv
import logging
from pathlib import Path
from datetime import date, datetime
import pyarrow.parquet as pq

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

PAYLOAD_MODE = os.getenv('TAXI_PRODUCER_PAYLOAD_MODE', 'raw').strip().lower()
PARQUET_BATCH_ROWS = int(os.getenv('TAXI_PRODUCER_PARQUET_BATCH_ROWS', '50000'))
FLUSH_EVERY_RECORDS = int(os.getenv('TAXI_PRODUCER_FLUSH_EVERY_RECORDS', '50000'))

producer = None


def get_producer():
    """Create KafkaProducer lazily so imports and tests do not open Kafka sockets."""
    global producer
    if producer is None:
        logger.info(f"Connecting to Kafka at: {KAFKA_SERVER}\n")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            api_version=(2, 8, 0),
            request_timeout_ms=30000,
            max_block_ms=60000,
            metadata_max_age_ms=300000,
            compression_type='gzip',
            batch_size=int(os.getenv('TAXI_PRODUCER_KAFKA_BATCH_SIZE', '131072')),
            linger_ms=int(os.getenv('TAXI_PRODUCER_LINGER_MS', '100')),
            buffer_memory=int(os.getenv('TAXI_PRODUCER_BUFFER_MEMORY', '268435456')),
            max_in_flight_requests_per_connection=5,
            acks=1,
            retries=3,
            retry_backoff_ms=1000
        )
    return producer

zone_lookup = None


def load_zone_lookup():
    """Load NYC taxi zones only for legacy enriched producer mode."""
    global zone_lookup
    if zone_lookup is not None:
        return zone_lookup

    zone_lookup = {}
    try:
        zone_df = pd.read_csv('data/taxi_zone_lookup.csv')
        for _, row in zone_df.iterrows():
            zone_lookup[int(row['LocationID'])] = {
                'Borough': row['Borough'],
                'Zone': row['Zone'],
                'Latitude': row['Latitude'],
                'Longitude': row['Longitude']
            }
        logger.info(f"Loaded {len(zone_lookup)} NYC taxi zones\n")
    except Exception as e:
        logger.warning(f"Zone lookup error: {e}. Continuing without zone names.\n")

    return zone_lookup


def get_zone_info(location_id):
    """Get zone info from lookup table"""
    lookup = load_zone_lookup()
    if location_id in lookup:
        zone_data = lookup[location_id]
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
        return 'nyc_taxi_yellow', 'yellow'
    elif 'green' in file_name.lower():
        return 'nyc_taxi_green', 'green'
    else:
        raise ValueError(f"Cannot determine taxi type from file: {file_name}")


# Checkpoint management
CHECKPOINT_FILE = ".producer_checkpoint.json"

def load_checkpoint():
    """Load files already sent"""
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed_files": [], "timestamp": None}

def save_checkpoint(processed_files):
    """Save checkpoint after successful file processing"""
    checkpoint = {
        "processed_files": processed_files,
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)


def serialize_value(value):
    """Convert parquet scalar values into JSON-safe source values."""
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if pd.isna(value):
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def serialize_raw_taxi_record(record, taxi_type, source_file, ingestion_timestamp):
    """Serialize source columns with minimal ingestion metadata."""
    message = {col: serialize_value(value) for col, value in record.items()}
    message['_taxi_type'] = taxi_type
    message['_source_file'] = source_file
    message['_ingestion_timestamp'] = ingestion_timestamp
    return message


def serialize_legacy_enriched_taxi_record(record, taxi_type, source_file, ingestion_timestamp):
    """Legacy producer payload kept as an opt-in fallback while Silver takes over enrichment."""
    message = {}
    
    # Standardize datetime columns
    if taxi_type == 'yellow':
        pickup_col = 'tpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime'
    else:  # green
        pickup_col = 'lpep_pickup_datetime'
        dropoff_col = 'lpep_dropoff_datetime'
    
    # Copy all columns with safe conversion
    for col, value in record.items():
        message[col] = serialize_value(value)
    
    # Add standardized datetime columns
    if pickup_col in message:
        message['pickup_datetime'] = message[pickup_col]
    if dropoff_col in message:
        message['dropoff_datetime'] = message[dropoff_col]
    
    # Add zone info for pickup location
    if 'PULocationID' in message and message['PULocationID']:
        try:
            pu_zone = get_zone_info(int(message['PULocationID']))
            if pu_zone:
                message.update({
                    'pickup_zone_borough': pu_zone.get('zone_borough'),
                    'pickup_zone_name': pu_zone.get('zone_name'),
                    'pickup_zone_lat': pu_zone.get('zone_lat'),
                    'pickup_zone_lon': pu_zone.get('zone_lon')
                })
        except:
            pass
    
    # Add zone info for dropoff location
    if 'DOLocationID' in message and message['DOLocationID']:
        try:
            do_zone = get_zone_info(int(message['DOLocationID']))
            if do_zone:
                message.update({
                    'dropoff_zone_borough': do_zone.get('zone_borough'),
                    'dropoff_zone_name': do_zone.get('zone_name'),
                    'dropoff_zone_lat': do_zone.get('zone_lat'),
                    'dropoff_zone_lon': do_zone.get('zone_lon')
                })
        except:
            pass
    
    # Add metadata
    message['_taxi_type'] = taxi_type
    message['_ingestion_timestamp'] = ingestion_timestamp
    message['_source_file'] = source_file
    
    return message


def build_taxi_message(record, taxi_type, source_file, ingestion_timestamp):
    """Build the Kafka payload according to producer mode."""
    if PAYLOAD_MODE == 'legacy_enriched':
        return serialize_legacy_enriched_taxi_record(record, taxi_type, source_file, ingestion_timestamp)
    if PAYLOAD_MODE != 'raw':
        logger.warning(f"Unknown TAXI_PRODUCER_PAYLOAD_MODE={PAYLOAD_MODE}; falling back to raw")
    return serialize_raw_taxi_record(record, taxi_type, source_file, ingestion_timestamp)


def stream_single_file(file_path, chunk_size=FLUSH_EVERY_RECORDS):
    """Stream parquet file in chunks with better error handling"""
    file_name = os.path.basename(file_path)
    topic, taxi_type = get_topic_from_filename(file_name)
    
    try:
        logger.info(f"Processing: {file_name} → {topic}")
        parquet_file = pq.ParquetFile(file_path)
        total_records = parquet_file.metadata.num_rows

        logger.info(f"   Total records: {total_records:,}")
        logger.info(f"   Parquet batch rows: {PARQUET_BATCH_ROWS:,} | Kafka flush every: {chunk_size:,}")

        start_time = time.time()
        sent_count = 0
        error_count = 0

        for batch in parquet_file.iter_batches(batch_size=PARQUET_BATCH_ROWS):
            ingestion_timestamp = datetime.now().isoformat()
            records = batch.to_pylist()

            for record in records:
                try:
                    message = build_taxi_message(record, taxi_type, file_name, ingestion_timestamp)
                    get_producer().send(topic, value=message)
                    sent_count += 1

                    if sent_count % chunk_size == 0:
                        get_producer().flush()
                        elapsed = time.time() - start_time
                        rate = sent_count / elapsed
                        pct = sent_count / total_records * 100
                        logger.info(f"[{file_name}] {sent_count:,}/{total_records:,} ({pct:.1f}%) - {rate:.0f} rec/s - errors: {error_count}")

                except Exception as e:
                    error_count += 1
                    if error_count % 100 == 0:
                        logger.warning(f"   Serialization errors: {error_count}")
                    continue

        logger.info(f"   Flushing final batch...")
        get_producer().flush()

        elapsed = time.time() - start_time
        logger.info(f"Completed: {file_name}")
        logger.info(f"Sent: {sent_count:,}/{total_records:,} records in {elapsed:.1f}s ({sent_count/elapsed:.0f} rec/s)")
        logger.info(f"Errors: {error_count}\n")

        return file_name

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

    checkpoint = load_checkpoint()
    processed_files = set(checkpoint["processed_files"])

    logger.info(f"Found {len(files)} total files\n")

    new_files = [f for f in files if os.path.basename(f) not in processed_files]

    if processed_files:
        logger.info(f"{len(processed_files)} files already processed")
        logger.info(f"Processing {len(new_files)} remaining files:\n")

    for f in new_files:
        logger.info(f"  - {os.path.basename(f)}")
    logger.info("")

    total_start = time.time()
    failed_files = []
    newly_processed = list(processed_files)

    for file_path in new_files:
        try:
            get_producer()._metadata.request_update()
            time.sleep(1)

            completed_file = stream_single_file(file_path)
            newly_processed.append(completed_file)
            save_checkpoint(newly_processed)

            logger.info("Cooling down...\n")
            time.sleep(5)

        except KafkaTimeoutError as e:
            logger.error(f"TIMEOUT: {os.path.basename(file_path)} - {e}")
            failed_files.append(os.path.basename(file_path))
            # Back off longer after Kafka metadata/send timeouts.
            time.sleep(10)
            continue

        except Exception as e:
            logger.error(f"ERROR: {os.path.basename(file_path)} - {type(e).__name__}: {e}")
            failed_files.append(os.path.basename(file_path))
            time.sleep(5)
            continue

    total_elapsed = time.time() - total_start
    logger.info("ALL FILES PROCESSED!")
    logger.info(f"Time: {total_elapsed/60:.1f} minutes")
    logger.info(f"Success: {len(newly_processed)}/{len(files)}")

    if failed_files:
        logger.warning(f"\n{len(failed_files)} files FAILED:")
        for f in failed_files:
            logger.warning(f"  - {f}")


if __name__ == "__main__":
    try:
        logger.info("=" * 60)
        logger.info("MetroPulse: NYC Taxi Data Producer")
        logger.info("=" * 60 + "\n")
        
        stream_all_taxis('data/raw/')
        
    except KeyboardInterrupt:
        logger.warning("\nStopped by user")
        logger.info("Flushing remaining messages...")
        if producer is not None:
            producer.flush()
        
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        
    finally:
        if producer is not None:
            producer.close()
        logger.info("Producer connection closed")
