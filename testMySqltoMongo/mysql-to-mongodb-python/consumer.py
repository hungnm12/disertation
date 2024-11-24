# consumer.py

import logging
import json

from confluent_kafka import KafkaError, KafkaException

from kafkaConfig import create_kafka_consumer  # Import Kafka consumer creation
from mysql_to_mongo import MySQLToMongoConverter  # Make sure this is imported

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DatabaseInfoConsumer:
    def __init__(self):
        # Use the function from kafka_config to create the Kafka consumer
        self.consumer = create_kafka_consumer()

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Wait for 1 second for messages
                if msg is None:
                    continue  # No message available within the timeout period
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue  # End of partition
                    else:
                        raise KafkaException(msg.error())
                else:
                    # Message successfully consumed
                    logger.info(f"Consumed message: {msg.value().decode('utf-8')}")
                    self.process_message(msg.value().decode('utf-8'))
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def process_message(self, message):
        try:
            # Parse the message containing the database credentials info
            db_info = json.loads(message)
            sql_config = {
                'host': db_info['sql_host'],
                'port': db_info['sql_port'],
                'user': db_info['sql_user'],
                'password': db_info['sql_password'],
                'database': db_info['sql_database'],
            }
            mongo_uri = db_info['mongo_uri']
            mongo_db_name = db_info['mongo_db_name']

            # Create and invoke the MySQLToMongoConverter
            converter = MySQLToMongoConverter(sql_config, mongo_uri)
            converter.convert()  # Convert data from MySQL to MongoDB
        except Exception as e:
            logger.error(f"Error processing message: {e}")

