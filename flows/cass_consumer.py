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

# Set the keyspace explicitly
session.execute("USE crypto_data")  # Use the correct keyspace name

# Define the CREATE TABLE statement
create_table_statement = f"""
    CREATE TABLE IF NOT EXISTS Crypto_asset (
        id TEXT PRIMARY KEY,
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
print("Table created")
# Execute the CREATE TABLE statement
session.execute(create_table_statement)


# Start consuming and writing data

            # Insert the data into Cassandra
            session.execute(insert_statement, (
                id_value,
                rank_value,
                symbol_value,
                name_value,
                supply_value,
                maxSupply_value,
                marketCapUsd_value,
                volumeUsd24Hr_value,
                priceUsd_value,
                changePercent24Hr_value,
                vwap24Hr_value
            ))
    
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Close the connections
session.shutdown()
cluster.shutdown()
