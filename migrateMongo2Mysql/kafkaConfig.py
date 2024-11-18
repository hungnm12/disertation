from confluent_kafka import Consumer
import json
import logging
from config import mongo_config
from migrate import run_migration

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kafka_consumer.log"),
        logging.StreamHandler()
    ]
)

# Consumer configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'db-connection-service-group',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer
consumer = Consumer(config)

# Subscribe to the topic
consumer.subscribe(['send-credential-topic'])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            logging.error(f"Error: {msg.error()}")
        else:
            # Log the message reception
            logging.info(f"Received message from Kafka: {msg.value().decode('utf-8')}")

            # Parse the received message (assuming JSON format)
            message = json.loads(msg.value().decode('utf-8'))
            mongo_config['url'] = message['mongo_url']
            mongo_config['username'] = message['username']
            mongo_config['password'] = message['password']
            mongo_config['database'] = message['database']

            # Log the updated configuration
            logging.info(f"Updated MongoDB configuration: {mongo_config}")

            # Run the migration process with the received configuration
            run_migration(mongo_config)

except KeyboardInterrupt:
    logging.info("Consumer interrupted and closing.")
finally:
    # Close the consumer
    consumer.close()
    logging.info("Kafka consumer closed.")
