import json
from kafka import KafkaConsumer
import itertools
from collections import defaultdict
# from pymongo import MongoClient

# Setup MongoDB connection
# client = MongoClient('localhost', 27017)
# db = client['apriori']
# results_collection = db['output']
def generate_candidates(frequent_itemsets, k):
    """Generate candidate itemsets of size k from frequent itemsets of size k-1."""
    items = set(itertools.chain.from_iterable(frequent_itemsets))
    return [tuple(sorted(combo)) for combo in itertools.combinations(items, k)]

def calculate_support(itemset, transactions):
    count = sum(1 for transaction in transactions if set(itemset).issubset(transaction))
    print(f"Checking support for {itemset}, count: {count}, total: {len(transactions)}")
    return count / len(transactions)

def apriori(transactions, min_support):
    print(f"Apriori running on {len(transactions)} transactions.")
    item_counts = defaultdict(int)
    for transaction in transactions:
        for item in transaction:
            item_counts[item] += 1

    frequent_itemsets = [[item] for item, count in item_counts.items() if count / len(transactions) >= min_support]
    print(f"Initial frequent itemsets: {frequent_itemsets}")

    k = 1
    while True:
        k += 1
        candidates = generate_candidates(frequent_itemsets, k)
        print(f"Generated {len(candidates)} candidates of size {k}.")
        current_frequent_itemsets = []
        for candidate in candidates:
            support = calculate_support(candidate, transactions)
            print(f"Candidate {candidate} has support {support}.")
            if support >= min_support:
                current_frequent_itemsets.append(candidate)
        if not current_frequent_itemsets:
            print("No new frequent itemsets found, stopping.")
            break
        frequent_itemsets = current_frequent_itemsets
        print(f"Found {len(current_frequent_itemsets)} frequent itemsets of size {k}.")
    # Store results in MongoDB
    # if all_frequent_itemsets:
    #     documents = [{'itemset': list(itemset), 'support': calculate_support(itemset, transactions)} for itemset in all_frequent_itemsets]
    #     results_collection.insert_many(documents)
    return frequent_itemsets


# Kafka consumer setup
consumer = KafkaConsumer(
    'topic_1',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='group1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Parameters
min_support = 0.01
window_size = 30  # Number of transactions to consider in the sliding window
slide_step = 10  # Number of new transactions before the window slides

# Buffer for transactions in the sliding window
transactions = []

for message in consumer:
    transaction = message.value.get('also_buy', [])
    if transaction:
        transactions.append(transaction)
        if len(transactions) >= window_size:
            try:
                # Process transactions in the current window
                print("Executing Apriori on the current window...")
                frequent_itemsets = apriori(transactions[-window_size:], min_support)
                if frequent_itemsets:
                    print("Frequent Itemsets and their counts:")
                    for itemset in frequent_itemsets:
                        count = sum(1 for trans in transactions[-window_size:] if set(itemset).issubset(set(trans)))
                        item_str = ', '.join(itemset)
                        print(f"{item_str} (count: {count})")
                else:
                    print("No frequent itemsets found.")
            except Exception as e:
                print(f"An error occurred during Apriori execution: {e}")
            # Slide the window
            transactions = transactions[-(window_size - slide_step):]
            # client.close()  # Close the MongoDB connection