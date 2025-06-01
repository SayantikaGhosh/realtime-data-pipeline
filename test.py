# test_gold.py
import pandas as pd
df = pd.read_parquet('./deltalake/gold/')
print(df.head())
