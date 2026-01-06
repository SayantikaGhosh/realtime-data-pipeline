import pandas as pd
from sqlalchemy import create_engine
import os

# Path to Gold Parquet folder inside container
gold_folder = "/tmp/delta/gold"

# Supabase Postgres connection string via Session Pooler (IPv4-friendly)
DATABASE_URL = "postgresql://postgres.siytrytcczkwjfzwhtdu:judo123adi1994@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"})

# 1️⃣ Read latest event_date from Supabase table (if it exists)
try:
    query = "SELECT MAX(event_date) AS latest_date FROM gold_table;"
    latest_date = pd.read_sql(query, engine)["latest_date"][0]
except Exception:
    latest_date = None

print(f"Latest event_date in Supabase: {latest_date}")

# 2️⃣ Check if Gold folder exists
if not os.path.exists(gold_folder):
    print(f"Error: Gold folder does not exist: {gold_folder}")
    exit(1)

# 3️⃣ Read all Parquet files
all_files = [os.path.join(gold_folder, f) for f in os.listdir(gold_folder) if f.endswith(".parquet")]

if not all_files:
    print("No Parquet files found in Gold folder. Exiting.")
    exit(0)

df_list = [pd.read_parquet(f) for f in all_files]
df = pd.concat(df_list, ignore_index=True)

print(f"Total rows in Gold layer: {len(df)}")

# Ensure event_date column is datetime
if "event_date" in df.columns:
    df["event_date"] = pd.to_datetime(df["event_date"])
else:
    print("Error: 'event_date' column not found in Gold data.")
    exit(1)

# 4️⃣ Filter only new rows based on latest_date
if latest_date is not None:
    latest_date = pd.to_datetime(latest_date)
    df_new = df[df["event_date"] > latest_date]
else:
    df_new = df

print(f"New rows to insert: {len(df_new)}")

# 5️⃣ Append new rows to Supabase
if not df_new.empty:
    try:
        df_new.to_sql("gold_table", engine, if_exists="append", index=False)
        print("New data successfully appended to Supabase!")
    except Exception as e:
        print(f"Error inserting data into Supabase: {e}")
else:
    print("No new data to append.")
