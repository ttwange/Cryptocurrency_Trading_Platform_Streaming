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

# Cassandra Configuration
cassandra_host = 'localhost'  # Update with your Cassandra host address

cluster = Cluster([cassandra_host])
session = cluster.connect()

# Create keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS crypto_data
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
""")

print("Keyspace created successfully!")

cassandra_keyspace = 'crypto_data'  # Update with your Cassandra keyspace
# Set the keyspace explicitly
session.execute("USE crypto_data")

# Define the CREATE TABLE statement
create_table_statement = f"""
    CREATE TABLE IF NOT EXISTS Crypto_asset (
        id UUID PRIMARY KEY,
        rank INT,
        symbol TEXT,
        name TEXT,
        supply DOUBLE,
        maxSupply DOUBLE,
        marketCapUsd DOUBLE,
        volumeUsd24Hr DOUBLE,
        priceUsd DOUBLE,
        changePercent24Hr DOUBLE,
        vwap24Hr DOUBLE
    )
"""

# Execute the CREATE TABLE statement
session.execute(create_table_statement)

# Create a prepared statement for inserting data into Cassandra
insert_statement = session.prepare("""
    INSERT INTO crypto_asset (
        id,
        rank,
        symbol,
        name,
        supply,
        maxSupply,
        marketCapUsd,
        volumeUsd24Hr,
        priceUsd,
        changePercent24Hr,
        vwap24Hr
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Start consuming and writing data



# # Close the connections
session.shutdown()
cluster.shutdown()
