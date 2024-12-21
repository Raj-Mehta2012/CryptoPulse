from confluent_kafka import Producer
import requests
import json
import time
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}
producer = Producer(conf)

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False,
    }
    response = requests.get(url, params=params)
    return response.json()

def produce_crypto_data():
    crypto_data = fetch_crypto_data()
    for crypto in crypto_data:
        message = {
            "name": crypto["name"],
            "symbol": crypto["symbol"],
            "current_price": crypto["current_price"],
            "price_change_percentage_24h": crypto["price_change_percentage_24h"]
        }
        producer.produce('crypto-data', key=crypto["symbol"], value=json.dumps(message))
        producer.flush()
        print(f"Sent: {message}")

# Produce data every minute
while True:
    produce_crypto_data()
    time.sleep(60)
