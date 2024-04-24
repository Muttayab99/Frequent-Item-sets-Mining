import json
import time
from kafka import KafkaProducer

# File path
file_path = 'processed_output.json'

# Kafka settings
bootstrap_servers = ['localhost:9092']
topics = ['topic_1', 'topic_2', 'topic_3']  # List of topics

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    linger_ms=10  
)

# Function to read data and send to Kafka
def send_transactions(file_path, topics):
    try:
        # Open and load the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)  # Load data from file as JSON

            # Send each record to all specified topics immediately
            for record in data:
                for topic in topics:
                    producer.send(topic, value=record)
                    print(f"Data sent to Kafka topic {topic}: {record}")
                producer.flush()  
                time.sleep(1)  

    except FileNotFoundError:
        print("File not found. Please check the file path.")
    except json.JSONDecodeError:
        print("Error decoding JSON. Check file content.")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        producer.close()  # Ensure the producer is closed properly

# Start the data sending process
send_transactions(file_path, topics)

