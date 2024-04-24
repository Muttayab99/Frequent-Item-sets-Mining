from kafka import KafkaConsumer, KafkaProducer
import json
from collections import defaultdict, deque
import time
# from pymongo import MongoClient

# Kafka configuration
topic_name = 'product_relationships'
bootstrap_servers = 'localhost:9092'

def safe_deserialize(x):
    try:
        return json.loads(x.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Failed to deserialize: {x}, error: {e}")
        return None

# Initialize Kafka consumer and producer
consumer = KafkaConsumer(
    'topic_3',
    bootstrap_servers=[bootstrap_servers],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=safe_deserialize
)

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Setup MongoDB connection
# client = MongoClient('localhost', 27017)
# db = client['pagerank']
# results_collection = db['rankings']

# Graph data structure
product_links = defaultdict(set)
rankings = defaultdict(lambda: 1.0)  # Start with a default rank of 1.0 for each product

# Constants for PageRank
damping_factor = 0.85
iterations = 5  # Number of iterations to perform for convergence in each window

def update_graph(transaction):
    """Update the graph structure based on the 'also_buy' relationship."""
    if transaction is None:
        return
    asin = transaction.get('asin')
    also_buy = transaction.get('also_buy', [])
    if asin and also_buy:
        for related_asin in also_buy:
            product_links[asin].add(related_asin)
            product_links[related_asin]  # Ensure each product is represented in the graph
    else:
        print(f"Invalid transaction format: {transaction}")

def calculate_pagerank():
    """Calculate the PageRank for each product."""
    global rankings
    num_nodes = len(product_links)
    for _ in range(iterations):
        new_rankings = defaultdict(float)
        for node, links in product_links.items():
            share = rankings[node] / len(links) if links else 0
            for linked_node in links:
                new_rankings[linked_node] += share
        # Apply the damping factor and handle sinks
        for node in product_links:
            new_rankings[node] = (1 - damping_factor) / num_nodes + damping_factor * new_rankings[node]
        rankings = new_rankings

def emit_rankings():
    """Emit the current rankings to another Kafka topic."""
    for product, rank in rankings.items():
        producer.send('product_rankings', {product: rank})
    producer.flush()

# Main loop processing messages from Kafka
window_size = 30  # Number of messages to accumulate before processing
window = deque(maxlen=window_size)

for message in consumer:
    transaction = message.value
    print(f"Received transaction type: {type(transaction)} - {transaction}")

    window.append(transaction)
    update_graph(transaction)

    if len(window) == window_size:
        calculate_pagerank()
        emit_rankings()
        # Optionally reset graph and rankings to handle non-stationary behavior
        product_links.clear()
        rankings.clear()
        rankings = defaultdict(lambda: 1.0)  # Reset rankings
        # Optionally store the results in MongoDB
        # results_collection.insert_many([{'asin': asin, 'rank': rank} for asin, rank in rankings.items()])

producer.close()
consumer.close()
# client.close()  # Optionally close the MongoDB connection if used
