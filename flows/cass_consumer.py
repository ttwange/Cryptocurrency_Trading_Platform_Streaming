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

# Create a prepared statement for inserting data into Cassandra
insert_statement = session.prepare("""
    INSERT INTO Crypto_asset (
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
for message in consumer:
    message_value = message.value.decode('utf-8')
    
    try:
        # Parse the JSON data as a list of objects
        data_list = json.loads(message_value)
        
        # Iterate through the list and process each JSON object
        for data in data_list:
            # Extract values from the JSON object
            id_value = data.get('id', None)
            rank_value = data.get('rank', None)
            symbol_value = data.get('symbol', None)
            name_value = data.get('name', None)
            supply_value = data.get('supply', None)
            maxSupply_value = data.get('maxSupply', None)
            marketCapUsd_value = data.get('marketCapUsd', None)
            volumeUsd24Hr_value = data.get('volumeUsd24Hr', None)
            priceUsd_value = data.get('priceUsd', None)
            changePercent24Hr_value = data.get('changePercent24Hr', None)
            vwap24Hr_value = data.get('vwap24Hr', None)
            print(id_value)
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
