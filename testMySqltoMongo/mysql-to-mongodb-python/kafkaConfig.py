# kafka_config.py

from confluent_kafka import Consumer, KafkaError, KafkaException

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server(s)
    'group.id': 'db-connection-service-group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start from the earliest message
}

# Kafka topic to consume from
topic = 'send-credential-topic'

def create_kafka_consumer():
    """
    This function creates and returns a Kafka Consumer instance
    with the provided consumer configuration.
    """
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    return consumer
