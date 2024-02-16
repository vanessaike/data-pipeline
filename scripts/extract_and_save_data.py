import os
import requests
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()

mongodb_user = os.getenv("MONGODB_USER")
mongodb_password = os.getenv("MONGODB_PASSOWORD")
mongodb_cluster = os.getenv("MONGODB_CLUSTER")
uri = f"mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_cluster}.g7v04mw.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

def connect_mongo(uri):
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, collection_name):
    collection = db[collection_name]
    return collection

def extract_api_data(url):
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        print(e)    

def insert_data(collection, data):
    collection.insert_many(data)
    return collection.count_documents({})

# function executions
try:
    connect_mongo(uri)
    db = create_connect_db(client, "test_products")
    collection = create_connect_collection(db, "products")
    data = extract_api_data("https://labdados.com/produtos")
    inserted_data = insert_data(collection, data)
    print(inserted_data, " documents inserted!")
    client.close()
except Exception as e:
    print(e)