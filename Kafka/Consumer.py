from confluent_kafka import Consumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
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

# MongoDB Configuration with SSL settings
mongodb_uri = "mongodb+srv://" + os.getenv('MONGODB_USER') + ":" + os.getenv('MONGODB_PASSWORD') + "@" + os.getenv('MONGODB_CLUSTER') + ".2cuow.mongodb.net/?retryWrites=true&w=majority&appName=" + os.getenv('MONGODB_CLUSTER') 

# Add SSL certificate verification bypass
if "?" in mongodb_uri:
    mongodb_uri += "&tlsAllowInvalidCertificates=true"
else:
    mongodb_uri += "?tlsAllowInvalidCertificates=true"

mongo_client = MongoClient(
    mongodb_uri,
    server_api=ServerApi('1')
)
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