import pandas as pd
import numpy as np
import os

# Define the output directory and ensure it exists
output_dir = os.path.join("data", "01_raw")
os.makedirs(output_dir, exist_ok=True)

# Define the output file path
output_file = os.path.join(output_dir, "mock_data.parquet")

# Create a sample DataFrame
data = {
    'id': range(1, 11),
    'feature1': np.random.rand(10),
    'feature2': np.random.randint(0, 100, 10),
    'category': np.random.choice(['A', 'B', 'C'], 10),
    'timestamp': pd.to_datetime(np.random.randint(1672531200, 1704067200, 10), unit='s') # Random timestamps in 2023
}
df = pd.DataFrame(data)

# Save the DataFrame to a Parquet file
print(f"Creating mock data Parquet file at: {output_file}")
df.to_parquet(output_file, index=False)
print("Mock data file created successfully.") 