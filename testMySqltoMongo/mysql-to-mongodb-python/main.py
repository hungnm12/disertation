# main.py

import logging
from consumer import DatabaseInfoConsumer  # To consume Kafka messages

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main():
    try:
        # Start consuming Kafka messages
        logger.info("Starting Kafka Consumer...")
        consumer = DatabaseInfoConsumer()
        consumer.consume_messages()
    except Exception as e:
        logger.error(f"Error running the main application: {e}")

if __name__ == "__main__":
    main()
