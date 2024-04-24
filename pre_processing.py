import pandas as pd
import json

# Define the batch size
batch_size = 1000
file_path = 'Sampled_Amazon_eta2.json'

def process_data(file_path, batch_size):
    processed_data = []
    with open(file_path, 'r') as file:
        # Load the dataset in batches
        for batch_data in pd.read_json(file, lines=True, chunksize=batch_size):
            # Parse JSON columns 'also_buy' and 'also_view'
            json_columns = ['also_buy', 'also_view']
            for col in json_columns:
                if col in batch_data.columns:
                    batch_data[col] = batch_data[col].apply(lambda x: json.loads(json.dumps(x)) if isinstance(x, list) else json.loads(x) if isinstance(x, str) else x)

            # Select only the 'asin' and 'also_buy' columns as required columns
            required_columns = ['asin', 'also_buy']
            batch_data = batch_data[required_columns]

            # Handle missing values if necessary
            batch_data = batch_data.dropna(subset=['also_buy'])

            # Convert each row to dictionary ensuring all data types are correct
            for index, row in batch_data.iterrows():
                # Serialize 'also_buy' to ensure it is always a JSON string
                if 'also_buy' in row and isinstance(row['also_buy'], list):
                    row['also_buy'] = json.dumps(row['also_buy'])

                processed_dict = {
                    'asin': row['asin'],
                    'also_buy': row['also_buy']
                }
                processed_data.append(processed_dict)

    # Serialize the entire processed data to JSON file
    with open('processed_output.json', 'w') as f_output:
        json.dump(processed_data, f_output, indent=4)

process_data(file_path, batch_size)