import pandas as pd
import numpy as np
import zipfile
import io
import requests
import os

# --- Step 1: download and unzip ---
# Create data folder
os.makedirs("data", exist_ok=True)

# Paths
local_zip_path = "data/18100004-eng.zip"
csv_path = "data/18100004-eng.csv"

# Download zip if not already on disk
if not os.path.exists(local_zip_path):
    print("Downloading zip file...")
    url = "https://www150.statcan.gc.ca/n1/tbl/csv/18100004-eng.zip"
    r = requests.get(url)
    with open(local_zip_path, "wb") as f:
        f.write(r.content)
    print(f"Saved zip to {os.path.abspath(local_zip_path)}")
else:
    print(f"Zip file already exists at {os.path.abspath(local_zip_path)}")

# Extract CSV if not already extracted
if not os.path.exists(csv_path):
    print("Extracting CSV from zip...")
    with zipfile.ZipFile(local_zip_path, "r") as z:
        # Assume first file inside zip is the CSV
        filename = z.namelist()[0]
        z.extract(filename, "data")
        # Rename to consistent csv_path
        os.rename(f"data/{filename}", csv_path)
    print(f"CSV saved to {os.path.abspath(csv_path)}")
else:
    print(f"CSV already exists at {os.path.abspath(csv_path)}")

df = pd.read_csv(csv_path)
print("DataFrame loaded successfully")

# --- Step 2: clean ---
# replace placeholder symbols with NaN
df.replace(['..', 'NaN', 'n/a', '', ' '], np.nan, inplace=True)

# convert REF_DATE to datetime
df['REF_DATE'] = pd.to_datetime(df['REF_DATE'], errors='coerce')

# convert VALUE to numeric
df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')

# strip whitespace from product names
df['Products and product groups'] = df['Products and product groups'].str.strip()

# print(df.sample(5))

df_wide = df.pivot_table(
    index=['REF_DATE', 'GEO'],
    columns='Products and product groups',
    values='VALUE'
).reset_index()

print(df_wide.sample(5))
