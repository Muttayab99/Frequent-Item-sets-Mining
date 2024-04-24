import json
import hashlib
from collections import defaultdict
from kafka import KafkaConsumer
# from pymongo import MongoClient

# Setup MongoDB connection
# client = MongoClient('localhost', 27017)
# db = client['pcy_algorithm']
# results_collection = db['frequent_pairs']
# Define the size of the hash table (number of buckets)
hash_table_size = 1000

# Initialize the hash table
hash_table = [0] * hash_table_size

# Function to generate hash values
def hash_value(item):
    return int(hashlib.md5(str(item).encode()).hexdigest(), 16) % hash_table_size

# Function to increment hash table counts
def increment_hash(item):
    hash_table[hash_value(item)] += 1

# Function to check if a pair is frequent
def is_frequent(pair, support_threshold):
    return hash_table[hash_value(pair)] >= support_threshold

# Function to process a single transaction
def process_transaction(transaction, support_threshold):
    # Generate hash values and increment hash table counts for each item
    for item in transaction:
        increment_hash(item)
    
    # Generate hash values for pairs and check if they are frequent
    for i in range(len(transaction)):
        for j in range(i + 1, len(transaction)):
            pair = tuple(sorted([transaction[i], transaction[j]]))
            if is_frequent(pair, support_threshold):
                print(f"Frequent pair found: {pair}")
# Store results in MongoDB
    # if frequent_pairs:
    #     documents = [{'pair': list(pair), 'count': hash_table[hash_value(pair)]} for pair in frequent_pairs]
    #     results_collection.insert_many(documents)

# Kafka settings
bootstrap_servers = ['localhost:9092']
topics = [ 'topic_2']

# Setup Kafka consumer
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: x  # No need to decode JSON since it's already deserialized
)

# Main function to consume messages and apply PCY algorithm
def consume_and_process(support_threshold):
    try:
        for message in consumer:
            transaction = message.value
            print(f"Received transaction: {transaction}")
            process_transaction(transaction, support_threshold)
    except KeyboardInterrupt:
        print("Consumer stopped.")

# Define the support threshold for frequent itemsets
support_threshold = 2  # Adjust this threshold as needed

# Start consuming messages and apply PCY algorithm
consume_and_process(support_threshold)
# client.close()  # Optionally close the MongoDB connection if used