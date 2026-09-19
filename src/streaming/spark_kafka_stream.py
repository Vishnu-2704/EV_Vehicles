from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# Create Spark session
spark = SparkSession.builder \
    .appName("EV Kafka Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema of Kafka event
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("energy_kwh", DoubleType(), True),
    StructField("amount_paid", DoubleType(), True),
    StructField("status", StringType(), True)
])

print("Starting Spark Kafka Streaming...")

# Read data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ev-charging-events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka JSON messages into columns
events = raw_stream.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Write streaming data to Bronze Parquet
query = events.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "data/processed/streaming/bronze/charging_events"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/ev-kafka-bronze-checkpoint"
    ) \
    .start()

print("Spark Streaming is running...")
print("Writing Kafka events to Bronze Parquet...")

query.awaitTermination()
