#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sys
import time
import csv
from datetime import datetime

from kafka import KafkaProducer

import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger


#####################################
# Define CSV Message Generator
#####################################

def generate_messages(csv_file_path):
    """
    Reads stock data from a CSV file and streams it as JSON messages.
    """
    try:
        with open(csv_file_path, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                json_message = {
                    "date": row["Date"],
                    "close_last": float(row["Close/Last"]),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                }
                yield json_message
    except Exception as e:
        logger.error(f"ERROR: Failed to read CSV file: {e}")
        sys.exit(3)


#####################################
# Define Main Function
#####################################

def main() -> None:
    logger.info("Starting Producer to stream stock data from CSV.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
        csv_file_path: str = config.get_stock_csv_path()  # Add CSV path in config
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 3. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 4. Streaming stock data from CSV.")
    try:
        for message in generate_messages(csv_file_path):
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
