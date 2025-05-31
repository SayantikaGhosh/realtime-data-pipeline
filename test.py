import pandas as pd
import glob

# Load Silver layer data
files = glob.glob('./deltalake/silver/part-*.parquet')
df = pd.concat([pd.read_parquet(f) for f in files])

print(df.head())
