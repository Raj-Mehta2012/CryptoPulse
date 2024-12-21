import streamlit as st
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

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

# MongoDB Configuration
db = mongo_client['CryptoData']
collection = db['Prices']

# Fetch Data
def get_top_gainers():
    data = list(collection.find().sort("price_change_percentage_24h", -1).limit(5))
    return data

# Streamlit Dashboard
st.title("Top Crypto Gainers")
st.write("Real-time updates of top-performing cryptocurrencies")

data = get_top_gainers()
for crypto in data:
    st.subheader(f"{crypto['name']} ({crypto['symbol']})")
    st.write(f"Current Price: ${crypto['current_price']}")
    st.write(f"24h Change: {crypto['price_change_percentage_24h']}%")
    st.write("---")
