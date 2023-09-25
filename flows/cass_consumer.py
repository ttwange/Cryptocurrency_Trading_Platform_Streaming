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

def create_keyspace(session):
    def create_keyspace(session):
       session.execute("""
        CREATE KEYSPACE IF NOT EXISTS crypto_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

# Cassandra Configuration
cassandra_host = 'localhost'  # Update with your Cassandra host address
cassandra_keyspace = 'your_keyspace'  # Update with your Cassandra keyspace

cluster = Cluster([cassandra_host])
session = cluster.connect()
# Define the CREATE TABLE statement
create_table_statement = f"""
    CREATE TABLE IF NOT EXISTS your_table (
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
    INSERT INTO your_table (
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


# # Start consuming and writing data
# Start consuming and writing data
for message in consumer:
    message_value = message.value.decode('utf-8')
    
    try:
        # Parse the JSON data
        data = json.loads(message_value)
        
        # Extract values from the JSON data
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

# # Close the connections
# session.shutdown()
# cluster.shutdown()
