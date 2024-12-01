# kafka_config.py
from confluent_kafka import Consumer
import json
import logging
from config import mongo_config, mysql_config, mysql_db_name, mongo_config_received, mysql_config_received


class KafkaConsumerService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka brokers
            'group.id': 'db-connection-service-group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['send-credential-nosql-topic', 'send-credential-sql-topic'])

    def process_message(self, msg):
        global mongo_config, mysql_config, mysql_db_name, mongo_config_received, mysql_config_received
        try:
            message = json.loads(msg.value().decode('utf-8'))
            logging.info(f"Received message: {message}")

            if msg.topic() == 'send-credential-nosql-topic':
                mongo_config['url'] = message['uri']
                mongo_config['db_name'] = message['collection']
                mongo_config_received = True
                logging.info("Updated MongoDB configuration")

            elif msg.topic() == 'send-credential-sql-topic':
                mysql_config['host'] = message['url']  # Directly use 'url' for hostname
                mysql_config['user'] = message['username']
                mysql_config['password'] = message['password']
                mysql_db_name['database'] = message['database']
                mysql_config_received = True
                logging.info("Updated MySQL configuration")

        except json.JSONDecodeError:
            logging.error("Failed to decode JSON from the message.")
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")

    def consume_and_update_configs(self):
        while not mongo_config_received or not mysql_config_received:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                continue
            self.process_message(msg)
        self.consumer.close()