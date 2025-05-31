from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType

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

# Read from Bronze Delta table as stream
df_bronze = spark.readStream \
    .format("delta") \
    .load("/tmp/delta/bronze")

# Simple cleaning/transformation (more can be added later)
df_silver = df_bronze.filter(
    col("user_id").isNotNull() &
    col("event_type").isNotNull() &
    col("product").isNotNull() &
    col("price").isNotNull() &
    col("timestamp").isNotNull()
)

# Write to Silver Delta table
df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/silver/_checkpoint") \
    .start("/tmp/delta/silver") \
    .awaitTermination()
