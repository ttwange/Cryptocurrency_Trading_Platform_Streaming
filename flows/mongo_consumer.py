import os
import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Kafka Consumer Configuration
kafka_bootstrap_servers = 'localhost:9092'  # Update with your Kafka broker address
kafka_topic = 'asset'

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id='asset_consumer_group',
    auto_offset_reset='earliest',  # You can choose 'earliest' or 'latest' based on your needs
)

client = MongoClient(
    mongodb_host,
    mongodb_port,
    username=mongodb_username,
    password=mongodb_password,
    authSource='admin',  # The authentication database
)

db = client[mongodb_database]
collection = db[mongodb_collection]

# Start consuming and writing data to MongoDB
for message in consumer:
    message_value = message.value.decode('utf-8')
    
    try:
        # Parse the JSON data as a list of objects
        data_list = json.loads(message_value)
        
        # Iterate through the list and process each JSON object
        for data in data_list:
            # Insert the data into MongoDB
            collection.insert_one(data)
            print("its working")
    
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Close the MongoDB connection
client.close()
