from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("EV Streaming Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting Streaming Silver...")

# Read Bronze Parquet as a streaming source
bronze_stream = spark.readStream \
    .format("parquet") \
    .schema("""
        transaction_id STRING,
        customer_id STRING,
        vehicle_id STRING,
        station_id STRING,
        start_time STRING,
        energy_kwh DOUBLE,
        amount_paid DOUBLE,
        status STRING
    """) \
    .load("data/processed/streaming/bronze/charging_events")

# Clean and validate the data
silver_stream = bronze_stream \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("vehicle_id").isNotNull()) \
    .filter(col("station_id").isNotNull()) \
    .filter(col("energy_kwh") > 0) \
    .filter(col("amount_paid") >= 0) \
    .filter(col("status").isin("COMPLETED", "CHARGING"))

# Write cleaned data to Silver Parquet
query = silver_stream.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "data/processed/streaming/silver/charging_events"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/ev-kafka-silver-checkpoint"
    ) \
    .start()

print("Streaming Silver is running...")
print("Writing cleaned events to Silver Parquet...")

query.awaitTermination()
