from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType
import time

# Define the schema (same as Bronze)
schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product", StringType()) \
    .add("price", FloatType()) \
    .add("timestamp", TimestampType())

# Create Spark session with Delta support
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_path = "/tmp/delta/bronze"
max_retries = 10
wait_seconds = 10

# Retry logic to wait for bronze table to be ready
for attempt in range(max_retries):
    try:
        # Try to read one row from bronze delta table to check if ready
        spark.read.format("delta").load(bronze_path).limit(1).collect()
        print(f"Bronze Delta table found. Proceeding with Silver processing.")
        break
    except Exception as e:
        print(f"Attempt {attempt+1}/{max_retries}: Bronze Delta table not ready. Retrying in {wait_seconds} seconds...")
        time.sleep(wait_seconds)
else:
    print(f"Bronze Delta table not found after {max_retries} attempts. Exiting.")
    spark.stop()
    exit(1)

# Now safe to read bronze delta table as stream
df_bronze = spark.readStream \
    .format("delta") \
    .load(bronze_path)

# Filter and deduplicate
df_silver = df_bronze.filter(
    col("user_id").isNotNull() &
    col("event_type").isin("click", "purchase") &
    col("product").isNotNull() &
    (col("price") > 0) &
    col("timestamp").isNotNull()
).dropDuplicates(["user_id", "event_type", "product", "price", "timestamp"])

# Write to Silver Delta table
df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/silver/_checkpoint") \
    .start("/tmp/delta/silver") \
    .awaitTermination()
