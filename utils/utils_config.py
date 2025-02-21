"""
Config Utility
File: utils/utils_config.py

This script provides the configuration functions for the project.

It centralizes the configuration management by loading environment variables from .env 
in the root project folder and constructing file paths using pathlib.

If you rename any variables in .env, remember to:
- recopy .env to .env.example (and hide the secrets)
- update the corresponding function in this module.
"""

#####################################
# Imports
#####################################

import os
import pathlib
from dotenv import load_dotenv
from .utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_broker_address() -> str:
    """Fetch Kafka broker address from environment or use default."""
    return os.getenv("KAFKA_BROKER", "localhost:9092")

def get_kafka_consumer_group() -> str:
    """Retrieve Kafka consumer group from environment variables."""
    return os.getenv("KAFKA_CONSUMER_GROUP", "default-consumer-group")

def get_kafka_consumer_group_id() -> str:
    """Retrieve Kafka consumer group ID from environment variables."""
    return os.getenv("KAFKA_CONSUMER_GROUP_ID", "default-group")

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    return os.getenv("KAFKA_TOPIC", "kafka_db")  # Fixed from BUZZ_TOPIC

def get_zookeeper_address() -> str:
    """Fetch Zookeeper address from environment or use default."""
    return os.getenv("ZOOKEEPER_ADDRESS", "localhost:2181")

def get_message_interval_seconds_as_int() -> int:
    """Fetch MESSAGE_INTERVAL_SECONDS from environment or use default."""
    return int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))

def get_base_data_path() -> pathlib.Path:
    """Fetch BASE_DATA_DIR from environment or use default."""
    project_root = pathlib.Path(__file__).parent.parent
    return project_root / os.getenv("BASE_DATA_DIR", "data")

def get_live_data_path() -> pathlib.Path:
    """Fetch LIVE_DATA_FILE_NAME from environment or use default."""
    return get_base_data_path() / os.getenv("LIVE_DATA_FILE_NAME", "project_live.json")

def get_stock_csv_path() -> str:
    """Fetch path for stock CSV file."""
    return "HistoricalData_1740009929602.csv"

def get_sqlite_path() -> pathlib.Path:
    """Fetch SQLITE_DB_FILE_NAME from environment or use default."""
    sqlite_path = get_base_data_path() / os.getenv("SQLITE_DB_FILE_NAME", "buzz.sqlite")
    logger.info(f"SQLITE_PATH: {sqlite_path}")
    return sqlite_path

#####################################
# Test Configuration (Optional)
#####################################

if __name__ == "__main__":
    logger.info("Testing configuration.")
    try:
        logger.info(f"ZOOKEEPER_ADDRESS: {get_zookeeper_address()}")
        logger.info(f"KAFKA_BROKER_ADDRESS: {get_kafka_broker_address()}")
        logger.info(f"KAFKA_TOPIC: {get_kafka_topic()}")
        logger.info(f"MESSAGE_INTERVAL_SECONDS: {get_message_interval_seconds_as_int()}")
        logger.info(f"LIVE_DATA_PATH: {get_live_data_path()}")
        logger.info("SUCCESS: Configuration function tests complete.")
    except Exception as e:
        logger.error(f"ERROR: Configuration function test failed: {e}")
