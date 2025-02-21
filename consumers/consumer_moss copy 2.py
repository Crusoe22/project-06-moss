#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import sys
from collections import defaultdict
from kafka import KafkaConsumer

# Import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available
from consumers.db_sqlite_case import init_db, insert_message

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

#####################################
# Function to Store Sentiment Scores
#####################################

def store_categorized_message(message: dict, file_path: str = "cat_sentiment.json"):
    """
    Store sentiment scores categorized by message category in a JSON file.

    Args:
        message (dict): The processed message.
        file_path (str): The file path where categorized sentiment scores will be stored.
    """
    try:
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as file:
                categorized_data = json.load(file)
        else:
            categorized_data = defaultdict(list)

        category = message.get("category", "uncategorized")
        sentiment_score = message.get("sentiment", 0.0)
        categorized_data.setdefault(category, []).append(sentiment_score)

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(categorized_data, file, indent=4)

        logger.info(f"Sentiment score stored in category '{category}'.")
    except Exception as e:
        logger.error(f"Error storing sentiment score: {e}")

#####################################
# Function to Process Messages
#####################################

def process_message(message: dict) -> dict:
    """
    Process and transform a single JSON message.

    Args:
        message (dict): The JSON message as a Python dictionary.

    Returns:
        dict: The processed message or None if an error occurs.
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

#####################################
# Function to Consume Kafka Messages
#####################################

def consume_messages_from_kafka(topic: str, kafka_url: str, group: str, sql_path: pathlib.Path):
    """
    Consume new messages from Kafka topic and process them.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    """
    try:
        verify_services()
        consumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
        is_topic_available(topic)
    except Exception as e:
        logger.error(f"ERROR: Kafka setup failed: {e}")
        sys.exit(1)

    logger.info(f"Kafka consumer started for topic '{topic}'")

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, sql_path)
                store_categorized_message(processed_message)
    except Exception as e:
        logger.error(f"ERROR: Kafka message consumption failed: {e}")
        sys.exit(1)

#####################################
# Main Function
#####################################

def main():
    """
    Main function to run the consumer process.
    """
    logger.info("Starting Kafka Consumer")
    
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        sqlite_path = config.get_sqlite_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read config: {e}")
        sys.exit(1)

    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("Deleted previous database file.")
        except Exception as e:
            logger.error(f"ERROR: Could not delete DB file: {e}")
            sys.exit(1)

    try:
        init_db(sqlite_path)
        consume_messages_from_kafka(topic, kafka_url, group_id, sqlite_path)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

#####################################
# Script Execution
#####################################

if __name__ == "__main__":
    main()
