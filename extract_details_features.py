import pandas as pd
import pyarrow.parquet as pa

# Load the data from a Parquet file
parquet_file = pa.read_table("amazon_fashion_clean_051624.parquet")
df = parquet_file.to_pandas()

# Normalize the 'details' column into a separate DataFrame
details_df = pd.json_normalize(df['details'])

# Calculate non-null counts for each column in details_df
nonnull_counts = details_df.notnull().sum()

# Filter columns where non-null counts are at least 100
columns_to_keep = nonnull_counts[nonnull_counts >= 100].index.tolist()
filtered_details_df = details_df[columns_to_keep]

# Include the 'title' column from the original dataframe
final_df = pd.concat([df['title'], filtered_details_df], axis=1)

# Save the filtered DataFrame to a CSV file
final_df.to_csv("filtered_details_with_titles.csv", index=False)

# Display the final DataFrame info to confirm
print(final_df.info())
