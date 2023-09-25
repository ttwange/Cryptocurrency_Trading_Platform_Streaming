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
#cassandra_keyspace = 'your_keyspace'  # Update with your Cassandra keyspace

cluster = Cluster([cassandra_host])
session = cluster.connect()

# Create a prepared statement for inserting data into Cassandra
#insert_statement = session.prepare("INSERT INTO your_table (column1, column2) VALUES (?, ?)")

# # Start consuming and writing data
for message in consumer:
    message_value = message.value.decode('utf-8')
    # Parse and process the message data as needed

    # Insert the data into Cassandra
    session.execute(insert_statement, (value1, value2))  # Update with your data

# # Close the connections
# session.shutdown()
# cluster.shutdown()
