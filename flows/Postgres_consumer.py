import os
import json
from kafka import KafkaConsumer
import psycopg2
from prefect import flow
import logging
from dotenv import load_dotenv
# Load environment variables from .env
load_dotenv()


@flow()
def process_kafka_to_postgres():
    # Kafka Consumer Configuration
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'asset'

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id='asset_consumer_group',
        auto_offset_reset='earliest',
    )

    # PostgreSQL Configuration
    postgres_host = os.getenv("postgres_host")
    postgres_port = os.getenv("postgres_port")
    postgres_database = os.getenv("postgres_database")
    postgres_user = os.getenv("postgres_user")
    postgres_password = os.getenv("postgres_password")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_database,
            user=postgres_user,
            password=postgres_password
        )
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL")
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise

    # Start consuming and writing data to PostgreSQL
    for message in consumer:
        message_value = message.value.decode('utf-8')
        
        try:
            # Parse the JSON data as a list of objects
            data_list = json.loads(message_value)
            
            # Iterate through the list and process each JSON object
            for data in data_list:
                # Define your INSERT SQL statement based on your modified table structure
                insert_sql = """
                    INSERT INTO Crypto_asset (
                        assetName, rank, symbol, supply, maxSupply, marketCapUsd,
                        volumeUsd24Hr, priceUsd, changePercent24Hr, vwap24Hr
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                # Insert the data into PostgreSQL
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

if __name__ == '__main__':
    process_kafka_to_postgres()