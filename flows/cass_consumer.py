import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

# Kafka Consumer Configuration
kafka_bootstrap_servers = 'localhost:9092'  # Update with your Kafka broker address
kafka_topic = 'asset'

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id='asset_consumer_group',
    auto_offset_reset='earliest',  # You can choose 'earliest' or 'latest' based on your needs
)

