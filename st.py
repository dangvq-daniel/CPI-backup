import pandas as pd
import psycopg2

# -----------------------------
# Load CSV and fill missing Province
# -----------------------------
csv_path = "cpi_long_with_location.csv"

# Read CSV
df = pd.read_csv(csv_path, dtype=str)  # all as string for safety

# Fill missing Province from GEO column if empty
df['Province'] = df['Province'].fillna(df['GEO'])

# Optional: also fill City if needed
df['City'] = df['City'].fillna('Unknown')

# Save a temporary cleaned CSV
cleaned_csv = "cpi_long_with_location_clean_filled.csv"
df.to_csv(cleaned_csv, index=False, encoding="utf-8")

# -----------------------------
# Connection details
# -----------------------------
user = "postgres"
host = "db.rtewftvldajjhqjbwwfx.supabase.co"
port = "5432"
database = "postgres"
password_path = "./password"

with open(password_path, "r") as f:
    password = f.readline().strip()

# -----------------------------
# Connect to Postgres
# -----------------------------
conn = psycopg2.connect(
    dbname=database,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()

# -----------------------------
# Fast bulk load CSV with COPY
# -----------------------------
table_name = "cpi_long_with_location"

with open(cleaned_csv, "r", encoding="utf-8") as f:
    # Skip the header row
    next(f)
    cur.copy_expert(
        f"""
        COPY {table_name} 
        FROM STDIN WITH CSV NULL '' 
        DELIMITER ',' 
        QUOTE '\"';
        """,
        f
    )

conn.commit()
cur.close()
conn.close()

print(f"✅ Fast bulk upload completed into '{table_name}' with missing Province filled")
