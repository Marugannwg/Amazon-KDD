import pyarrow.parquet as pq
import pandas as pd
import json

# Path to your Parquet file
parquet_file_path = 'data/amazon_fashion_clean_051624.parquet' # UPDATE INPUT PATH HERE
parquet_file = pq.ParquetFile(parquet_file_path)

# Batch size for reading
batch_size = 20000 # choose a value that works for your system

# Dictionary to hold all product details
product_details = {}

# Iterator to go through the Parquet file in batches
iterator = parquet_file.iter_batches(batch_size=batch_size)

batch_count = 0

for batch in iterator:
    # Convert the current batch to a pandas DataFrame
    df = batch.to_pandas()
    
    # Extract the first large image link
    df['first_large_image'] = df['images'].apply(lambda x: x.tolist()[0]['large'] if x.size > 0 else None)
    
    # Filter the columns needed for the final output
    df_final = df[['parent_asin', 'title', 'first_large_image']]
    
    # Update the dictionary with the data from this batch
    for _, row in df_final.iterrows():
        asin = row['parent_asin']
        product_details[asin] = {'title': row['title'], 'image_link': row['first_large_image']}

    batch_count += 1

    print(f'Processed batch {batch_count}')

# Path where you want to save the JSON file
json_file_path = 'output/product_details.json' ## UPDATE OUTPUT PATH HERE

# Write the dictionary to a JSON file
with open(json_file_path, 'w') as json_file:
    json.dump(product_details, json_file, indent=4, sort_keys=True)

print(f'Product details have been successfully written to {json_file_path}')