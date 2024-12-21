from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

uri = "mongodb+srv://" + os.getenv('MONGODB_USER') + ":" + os.getenv('MONGODB_PASSWORD') + "@" + os.getenv('MONGODB_CLUSTER') + ".2cuow.mongodb.net/?retryWrites=true&w=majority&appName=" + os.getenv('MONGODB_CLUSTER') 

# Add tlsAllowInvalidCertificates=true to the connection string
if "?" in uri:
    uri += "&tlsAllowInvalidCertificates=true"
else:
    uri += "?tlsAllowInvalidCertificates=true"

client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)