import os
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv
# Load environment variables from .env
load_dotenv()

# Kafka Consumer Configuration
kafka_bootstrap_servers = 'localhost:9092'  
kafka_topic = 'asset'

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id='asset_consumer_group',
    auto_offset_reset='earliest',  
)

# MongoDB Configuration
mongodb_host = os.getenv("mongodb_host")
mongodb_port = os.getenv("mongodb_port")
mongodb_database = os.getenv("mongodb_database") 
mongodb_collection = os.getenv("mongodb_collection")
mongodb_username = os.getenv("mongodb_username")
mongodb_password = os.getenv("mongodb_password")

client = MongoClient(
    mongodb_host,
    mongodb_port,
    username=mongodb_username,
    password=mongodb_password,
    authSource='admin', 
)

db = client[mongodb_database]
collection = db[mongodb_collection]

# Start consuming and writing data to MongoDB
for message in consumer:
    message_value = message.value.decode('utf-8')
    
    try:
        data_list = json.loads(message_value)
        for data in data_list:
            collection.insert_one(data)
            print("its working")
    
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Close the MongoDB connection
client.close()
