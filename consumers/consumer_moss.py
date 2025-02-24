#####################################
# Import Modules
#####################################

import json
import os
import sys
from kafka import KafkaConsumer
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

import matplotlib.pyplot as plt

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# File path for storing stock data
STOCK_DATA_FILE = r"C:\Users\nolan\Documents\Streamin Data NWMU\project-06-moss\stock_data.json"




#####################################
# Create Chart
#####################################


# Initialize Matplotlib for live updates
fig, ax = plt.subplots()
plt.ion()  # Turn on interactive mode


# Initialize lists to store timestamps and sentiment scores
dates = []
close_prices = []


"""For the close_last and date data I want to create a line chart that holds the 
date as the x axis and the close_last as the y axis"""

def update_chart():
    """Update the live line chart with sentiment trends."""
    ax.clear()

    # Plot sentiment over time
    ax.plot(dates, close_prices, marker='o', linestyle='-', color="blue")

    ax.set_xlabel("Date")
    ax.set_ylabel("Price at close")
    ax.set_title("Daily close price for S&P 500")

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)




#####################################
# Store Stock Data
#####################################

def process_message(msg_value):
    """Process a Kafka message: validate and store stock data."""
    try:
        logger.info(f"Processing stock data: {msg_value}")

        # Ensure msg_value is a dictionary
        if isinstance(msg_value, str):
            message_dict = json.loads(msg_value)  # Only decode if it's a string
        else:
            message_dict = msg_value  

        date_value = message_dict.get("date", "unknown")
        close_last_value = message_dict.get("close_last", 0.0)

        if date_value != "unknown":
            dates.append(date_value)  # FIXED list name
            close_prices.append(close_last_value)  # FIXED list name

            # Keep only the last 100 data points
            if len(dates) > 30:
                dates.pop(0)
                close_prices.pop(0)

            update_chart()

        store_stock_data(message_dict)

    except Exception as e:
        logger.error(f"Error processing message: {e}")



def store_stock_data(message: dict):
    """
    Store stock data messages in a JSON file.

    Args:
        message (dict): The stock data message.

    """
    # Clear the JSON file when the script starts
    with open(STOCK_DATA_FILE, "w", encoding="utf-8") as file:
        json.dump([], file)
    logger.info("Stock data file cleared.") # log clearing

    try:
        # Load existing data if file exists
        if os.path.exists(STOCK_DATA_FILE):
            with open(STOCK_DATA_FILE, "r", encoding="utf-8") as file:
                stock_data = json.load(file)
        else:
            stock_data = []

        # Append new message
        stock_data.append(message)

        # Save updated data
        with open(STOCK_DATA_FILE, "w", encoding="utf-8") as file:
            json.dump(stock_data, file, indent=4)

        logger.info("Stock data stored successfully.")
    except Exception as e:
        logger.error(f"Error storing stock data: {e}")


#####################################
# Process Messages from Kafka
#####################################
def consume_messages_from_kafka(topic: str, kafka_url: str, group: str):
    """
    Consume new messages from Kafka topic and store them in the JSON file.
    """
    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(1)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # FIXED
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(1)

    logger.info("Step 3. Verify topic exists.")
    try:
        is_topic_available(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}")
        sys.exit(1)

    logger.info("Step 4. Process messages.")

    try:
        for message in consumer:
            msg_value = message.value  # Already deserialized JSON
            logger.info(f"Raw Message Received: {msg_value}")
            process_message(msg_value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR in message consumption: {e}")
    finally:
        consumer.close()
        logger.info("Consumer shutting down.")


#####################################
# Define Main Function
#####################################

def main():
    """
    Main function to run the consumer process.

    Reads configuration and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")

    logger.info("STEP 1. Read environment variables.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Begin consuming and storing messages.")
    consume_messages_from_kafka(topic, kafka_url, group_id)


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
