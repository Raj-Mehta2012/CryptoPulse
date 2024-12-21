from confluent_kafka import Consumer
from pymongo import MongoClient
import json
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'group.id': 'crypto-group',
    'auto.offset.reset': 'earliest',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}
consumer = Consumer(conf)
consumer.subscribe(['crypto-data'])

# MongoDB Configuration
mongo_client = MongoClient('<mongodb-connection-string>')
db = mongo_client['CryptoData']
collection = db['Prices']

# Consume and Insert Data
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        crypto_data = json.loads(msg.value().decode('utf-8'))
        collection.update_one(
            {"symbol": crypto_data["symbol"]},
            {"$set": crypto_data},
            upsert=True
        )
        print(f"Upserted: {crypto_data}")
except KeyboardInterrupt:
    print("Shutting down...")
finally:
    consumer.close()
