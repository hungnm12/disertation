from confluent_kafka import Consumer
import json

from config import mongo_config
from migrate import main

# ... other imports ...

# Consumer configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer
consumer = Consumer(config)

# Subscribe to the topic
consumer.subscribe(['your-topic-name'])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            print(f"Error: {msg.error()}")
        else:
            # Parse the received message (assuming JSON format)
            message = json.loads(msg.value().decode('utf-8'))
            mongo_config['url'] = message['mongo_url']
            mongo_config['username'] = message['username']
            mongo_config['password'] = message['password']
            mongo_config['database'] = message['database']

            # Now proceed with the data migration using the updated mongo_config
            main()
except KeyboardInterrupt:
    pass

finally:
    consumer.close()