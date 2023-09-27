import json
from kafka import KafkaConsumer
import psycopg2

# Kafka Consumer Configuration
kafka_bootstrap_servers = 'localhost:9092'  
kafka_topic = 'asset'

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id='asset_consumer_group',
    auto_offset_reset='earliest',  
)

postgres_host = 'localhost'
postgres_port = 5432
postgres_database = 'test'
postgres_user = 'api_user'
postgres_password = 'api_user'

try:
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password
    )
    cursor = conn.cursor()
    print("Connected to PostgreSQL")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit(1)

for message in consumer:
    message_value = message.value.decode('utf-8')
    
    try:
        data_list = json.loads(message_value)
        
        for data in data_list:
            insert_sql = """
                INSERT INTO Crypto_asset (
                    assetName, rank, symbol, supply, maxSupply, marketCapUsd,
                    volumeUsd24Hr, priceUsd, changePercent24Hr, vwap24Hr
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            cursor.execute(insert_sql, (
                data['id'],  # Insert "id" from Kafka into "assetName" column
                data['rank'], data['symbol'], data['supply'], data['maxSupply'],
                data['marketCapUsd'], data['volumeUsd24Hr'], data['priceUsd'],
                data['changePercent24Hr'], data['vwap24Hr']
            ))
            conn.commit()
            print("Data inserted into PostgreSQL")
    
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

cursor.close()
conn.close()