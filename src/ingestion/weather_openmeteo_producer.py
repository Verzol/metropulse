"""
MetroPulse: Open-Meteo Historical Weather Data Producer
Fetches NYC historical weather data (2023-2024) and streams to Kafka
"""

import os
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv
import requests
from typing import Dict, List
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
WEATHER_TOPIC = 'weather_stream'

# NYC Coordinates (Manhattan center)
NYC_LAT = 40.7128
NYC_LON = -74.0060
NYC_TIMEZONE = "America/New_York"

# Open-Meteo API Configuration
OPENMETEO_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_PARAMS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover"
]

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


def get_weather_description(weather_code: int) -> str:
    """
    Convert WMO weather code to human-readable description
    Reference: https://open-meteo.com/en/docs#weather_code
    """
    weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Foggy",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        71: "Slight snow",
        73: "Moderate snow",
        75: "Heavy snow",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail",
    }
    return weather_codes.get(weather_code, "Unknown")


def fetch_weather_data(start_date: str, end_date: str) -> Dict:
    """
    Fetch weather data from Open-Meteo API for a date range
    Dates format: YYYY-MM-DD
    """
    params = {
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(WEATHER_PARAMS),
        "timezone": NYC_TIMEZONE,
        "temperature_unit": "fahrenheit"
    }

    logger.info(f"Fetching weather data from {start_date} to {end_date}...")
    
    try:
        response = requests.get(OPENMETEO_BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if "hourly" not in data:
            raise ValueError("No hourly data in API response")
        
        logger.info(f"Received {len(data['hourly']['time'])} hourly records")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch weather data: {e}")
        raise


def process_weather_data(data: Dict) -> List[Dict]:
    """
    Process API response into Kafka message format
    """
    messages = []
    hourly = data.get("hourly", {})
    
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humidity = hourly.get("relative_humidity_2m", [])
    precipitation = hourly.get("precipitation", [])
    weather_codes = hourly.get("weather_code", [])
    wind_speed = hourly.get("wind_speed_10m", [])
    wind_dir = hourly.get("wind_direction_10m", [])
    cloud_cover = hourly.get("cloud_cover", [])
    
    for i, time_str in enumerate(times):
        message = {
            "timestamp": time_str,  # ISO format with timezone
            "latitude": NYC_LAT,
            "longitude": NYC_LON,
            "location": "NYC_Manhattan",
            "temperature_f": temps[i] if temps[i] is not None else None,
            "humidity_percent": humidity[i] if humidity[i] is not None else None,
            "precipitation_mm": precipitation[i] if precipitation[i] is not None else None,
            "weather_code": weather_codes[i] if weather_codes[i] is not None else None,
            "weather_description": get_weather_description(weather_codes[i]) if weather_codes[i] is not None else None,
            "wind_speed_kmh": wind_speed[i] if wind_speed[i] is not None else None,
            "wind_direction_deg": wind_dir[i] if wind_dir[i] is not None else None,
            "cloud_cover_percent": cloud_cover[i] if cloud_cover[i] is not None else None,
            "_ingestion_timestamp": datetime.now().__str__(),
            "_source": "open_meteo"
        }
        messages.append(message)
    
    return messages


def stream_weather_to_kafka(messages: List[Dict]) -> int:
    """
    Stream weather messages to Kafka
    """
    total_sent = 0
    
    logger.info(f"Streaming {len(messages)} weather records to Kafka topic: {WEATHER_TOPIC}\n")
    
    start_time = time.time()
    
    try:
        for idx, message in enumerate(messages):
            producer.send(WEATHER_TOPIC, value=message)
            total_sent += 1
            
            if (idx + 1) % 24 == 0:  # Every 24 hours
                elapsed = time.time() - start_time
                rate = (idx + 1) / elapsed if elapsed > 0 else 0
                progress_pct = (idx + 1) / len(messages) * 100
                logger.info(f"   {idx + 1:,}/{len(messages):,} ({progress_pct:.1f}%) - {rate:.0f} records/sec")
        
        producer.flush()
        
        elapsed = time.time() - start_time
        logger.info(f"\n✓ Streamed {total_sent:,} weather records in {elapsed:.1f}s ({total_sent/elapsed:.0f} records/sec)")
        
    except Exception as e:
        logger.error(f"Error streaming weather data: {e}")
        raise
    
    return total_sent


def main():
    """
    Main function: Fetch and stream historical weather data
    """
    try:
        # Historical data range: 2023-2024 (NYC taxi data period)
        start_date = "2023-01-01"
        end_date = "2024-12-31"
        
        logger.info("MetroPulse: Open-Meteo Weather Data Producer")
        logger.info(f"Location: NYC (Lat: {NYC_LAT}, Lon: {NYC_LON})")
        logger.info(f"Period: {start_date} to {end_date}")
        logger.info(f"Timezone: {NYC_TIMEZONE}")
        logger.info(f"Kafka Topic: {WEATHER_TOPIC}")
        
        # Fetch weather data
        raw_data = fetch_weather_data(start_date, end_date)
        
        # Process into messages
        messages = process_weather_data(raw_data)
        logger.info(f"Processed {len(messages)} weather messages\n")
        
        # Stream to Kafka
        total_sent = stream_weather_to_kafka(messages)
        
        logger.info("Weather data streaming COMPLETED!")
        logger.info(f"Total records sent: {total_sent:,}")
        logger.info(f"Kafka topic: {WEATHER_TOPIC}")
        logger.info("Ready for Bronze layer ingestion")
        
    except KeyboardInterrupt:
        logger.warning("\nStopped by user")
        producer.flush()
        
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        raise
        
    finally:
        producer.close()
        logger.info("Producer connection closed")


if __name__ == "__main__":
    main()
