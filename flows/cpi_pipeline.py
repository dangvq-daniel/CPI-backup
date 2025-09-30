# cpi_pipeline.py
import os
import requests
import zipfile
import pandas as pd
import numpy as np
import hashlib
from sqlalchemy import create_engine
from datetime import datetime

# -----------------------------
# CONFIGURATION
# -----------------------------
DATA_DIR = "data"
ZIP_FILE = os.path.join(DATA_DIR, "18100004-eng.zip")
CSV_FILE = os.path.join(DATA_DIR, "18100004-eng.csv")

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD_PATH = "./password"  # store securely
POSTGRES_HOST = "db.rtewftvldajjhqjbwwfx.supabase.co"
POSTGRES_PORT = "5432"
POSTGRES_DB = "postgres"
TABLE_NAME = "cpi_long"

STATS_CAN_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/18100004-eng.zip"

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def download_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Download ZIP
    if not os.path.exists(ZIP_FILE):
        print("Downloading CPI zip...")
        r = requests.get(STATS_CAN_URL)
        r.raise_for_status()
        with open(ZIP_FILE, "wb") as f:
            f.write(r.content)
        print(f"Saved zip to {ZIP_FILE}")
    else:
        print("Zip already exists.")
    
    # Extract CSV
    if not os.path.exists(CSV_FILE):
        print("Extracting CSV from zip...")
        with zipfile.ZipFile(ZIP_FILE, "r") as z:
            filename = z.namelist()[0]
            z.extract(filename, DATA_DIR)
            os.rename(os.path.join(DATA_DIR, filename), CSV_FILE)
        print(f"CSV saved to {CSV_FILE}")
    else:
        print("CSV already extracted.")

def encode_col(name: str) -> str:
    """Encode product names into safe column strings."""
    hash_suffix = hashlib.md5(name.encode()).hexdigest()[:8]
    safe_name = ''.join(c if c.isalnum() else '_' for c in name)
    return (safe_name[:40] + '_' + hash_suffix).lower()

def clean_transform(df: pd.DataFrame) -> pd.DataFrame:
    # Replace placeholder symbols with NaN
    df.replace(['..', 'NaN', 'n/a', '', ' '], np.nan, inplace=True)
    
    # Convert types
    df['REF_DATE'] = pd.to_datetime(df['REF_DATE'], errors='coerce')
    df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
    
    # Encode product names
    df['Encoded_Product'] = df['Products and product groups'].apply(encode_col)
    
    # Sort for pct_change calculations
    df = df.sort_values(['GEO', 'Encoded_Product', 'REF_DATE'])
    
    # Compute MoM and YoY
    df['MoM'] = df.groupby(['GEO', 'Encoded_Product'])['VALUE'].pct_change() * 100
    df['YoY'] = df.groupby(['GEO', 'Encoded_Product'])['VALUE'].pct_change(12) * 100
    
    # Forward/backward fill small gaps
    df[['VALUE', 'MoM', 'YoY']] = df[['VALUE', 'MoM', 'YoY']].fillna(method='ffill').fillna(method='bfill')
    
        # -----------------------------
    # Extract City and Province from GEO
    # -----------------------------
    def extract_city_province(geo: str):
        if pd.isna(geo):
            return pd.NA, pd.NA
        parts = [p.strip() for p in geo.split(",", 1)]
        city = parts[0] if len(parts) > 0 else pd.NA
        province = parts[1] if len(parts) > 1 else city
        return city, province

    df[['City', 'Province']] = df.apply(
        lambda row: extract_city_province(row['GEO']) 
                    if pd.isna(row.get('City')) or pd.isna(row.get('Province')) 
                    else (row['City'], row['Province']),
        axis=1,
        result_type='expand'
    )

    # Fill remaining missing values
    df['City'] = df['City'].fillna('Unknown')
    df['Province'] = df['Province'].fillna('Unknown')
    return df

def load_to_postgres(df: pd.DataFrame):
    # Load password securely
    with open(POSTGRES_PASSWORD_PATH, "r") as f:
        password = f.readline().strip()
    
    db_url = f"postgresql://{POSTGRES_USER}:{password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    engine = create_engine(db_url, pool_pre_ping=True)
    
    # Upsert logic: replace entire table or append incrementally
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows into {TABLE_NAME}.")

# -----------------------------
# PIPELINE
# -----------------------------
def run_pipeline():
    print("=== CPI Pipeline Started ===")
    
    # Step 1: Download and extract CSV
    download_csv()
    
    # Step 2: Load CSV
    df = pd.read_csv(CSV_FILE)
    print(f"Loaded {len(df)} rows from CSV.")
    
    # Step 3: Clean & transform
    df_clean = clean_transform(df)
    print("Data cleaned and transformed.")
    
    # Step 4: Load into Postgres
    load_to_postgres(df_clean)
    
    print("=== CPI Pipeline Finished ===")

# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    run_pipeline()
