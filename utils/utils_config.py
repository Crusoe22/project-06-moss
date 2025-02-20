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


def get_zookeeper_address() -> str:
    """Fetch ZOOKEEPER_ADDRESS from environment or use default."""
    return os.getenv("ZOOKEEPER_ADDRESS", "localhost:2181")


def get_kafka_broker_address() -> str:
    """Fetch KAFKA_BROKER_ADDRESS from environment or use default."""
    return os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")


def get_kafka_topic() -> str:
    """Fetch BUZZ_TOPIC from environment or use default."""
    return os.getenv("BUZZ_TOPIC", "buzzline")


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
    return "HistoricalData_1740009929602.csv"


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
