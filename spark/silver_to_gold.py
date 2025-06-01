from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, sum as spark_sum, count, avg
)

# Create Spark session
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Silver Delta table
df_silver = spark.readStream.format("delta").load("/tmp/delta/silver")

# Add date column for aggregation
df_silver = df_silver.withColumn("date", to_date("timestamp"))

# Aggregate metrics per product per date
agg_df = df_silver.groupBy("date", "product").agg(
    count(when(col("event_type") == "click", True)).alias("total_clicks"),
    count(when(col("event_type") == "purchase", True)).alias("total_purchases"),
    spark_sum(when(col("event_type") == "purchase", col("price"))).alias("total_revenue"),
    avg(when(col("event_type") == "purchase", col("price"))).alias("average_price")
)

# Add conversion_rate safely (handle division by zero)
agg_df = agg_df.withColumn(
    "conversion_rate",
    when(col("total_clicks") > 0, col("total_purchases") / col("total_clicks")).otherwise(0.0)
)

# Write to Gold Delta table
agg_df.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/delta/gold/_checkpoint") \
    .start("/tmp/delta/gold") \
    .awaitTermination()
