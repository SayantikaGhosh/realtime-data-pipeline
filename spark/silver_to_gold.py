from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, when, to_date, approx_count_distinct
import time

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

silver_path = "/tmp/delta/silver"

# Retry logic for Silver table readiness
for _ in range(10):
    try:
        spark.read.format("delta").load(silver_path).limit(1).collect()
        break
    except:
        time.sleep(5)
else:
    print("Silver table not ready. Exiting.")
    spark.stop()
    exit(1)

# Read Silver as streaming DataFrame
df_silver = spark.readStream.format("delta").load(silver_path)

# Add derived column for event date
df_silver = df_silver.withColumn("event_date", to_date(col("timestamp")))

# Gold table aggregation with extended metrics
df_gold = df_silver.groupBy(
    "product_id",
    "product_name",
    "category",
    "brand",
    "event_date"
).agg(
    count("*").alias("total_events_count"),                                # Total events (clicks + purchases)
    spark_sum(when(col("event_type")=="click", 1).otherwise(0)).alias("click_count"),
    spark_sum(when(col("event_type")=="purchase", 1).otherwise(0)).alias("purchase_count"),
    spark_sum(col("price")).alias("total_revenue_usd"),
    spark_sum(when(col("event_type")=="purchase", col("price")).otherwise(0)).alias("purchase_revenue_usd"),
    approx_count_distinct("user_id").alias("unique_users")                # Streaming-safe approx distinct count
)

# Additional calculated metrics
df_gold = df_gold.withColumn("avg_revenue_per_event", col("total_revenue_usd") / col("total_events_count")) \
                 .withColumn("conversion_rate", col("purchase_count") / col("total_events_count")) \
                 .withColumn("avg_price_per_purchase", col("purchase_revenue_usd") / col("purchase_count")) \
                 .withColumn("click_to_purchase_ratio", col("click_count") / col("purchase_count")) \
                 .withColumn("events_per_user", col("total_events_count") / col("unique_users")) \
                 .withColumn("purchases_per_user", col("purchase_count") / col("unique_users"))

# Write Gold table as Delta
df_gold.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/delta/gold/_checkpoint") \
    .start("/tmp/delta/gold") \
    .awaitTermination()
