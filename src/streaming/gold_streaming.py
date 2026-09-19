from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, current_timestamp, lit

# Create Spark session
spark = SparkSession.builder \
    .appName("EV Streaming Gold") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting Streaming Gold...")

# Read Silver Parquet as streaming source
silver_stream = spark.readStream \
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
    .load("data/processed/streaming/silver/charging_events")


# Process every micro-batch
def process_batch(batch_df, batch_id):

    print("Processing Gold Batch:", batch_id)

    if batch_df.isEmpty():
        return

    # -----------------------------
    # STATION ANALYTICS
    # -----------------------------

    station_gold = batch_df \
        .groupBy("station_id") \
        .agg(
            sum("energy_kwh").alias("total_energy_kwh"),
            sum("amount_paid").alias("total_revenue"),
            count("transaction_id").alias("charging_sessions")
        ) \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("processed_at", current_timestamp())

    station_gold.write \
        .format("parquet") \
        .mode("append") \
        .save("data/processed/streaming/gold/station_analytics")


    # -----------------------------
    # CUSTOMER ANALYTICS
    # -----------------------------

    customer_gold = batch_df \
        .groupBy("customer_id") \
        .agg(
            sum("energy_kwh").alias("total_energy_kwh"),
            sum("amount_paid").alias("total_amount_paid"),
            count("transaction_id").alias("charging_sessions")
        ) \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("processed_at", current_timestamp())

    customer_gold.write \
        .format("parquet") \
        .mode("append") \
        .save("data/processed/streaming/gold/customer_analytics")


    # -----------------------------
    # VEHICLE ANALYTICS
    # -----------------------------

    vehicle_gold = batch_df \
        .groupBy("vehicle_id") \
        .agg(
            sum("energy_kwh").alias("total_energy_kwh"),
            sum("amount_paid").alias("total_amount_paid"),
            count("transaction_id").alias("charging_sessions")
        ) \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("processed_at", current_timestamp())

    vehicle_gold.write \
        .format("parquet") \
        .mode("append") \
        .save("data/processed/streaming/gold/vehicle_analytics")


# Start Gold Streaming
query = silver_stream.writeStream \
    .foreachBatch(process_batch) \
    .option(
        "checkpointLocation",
        "/tmp/ev-kafka-gold-checkpoint-v4"
    ) \
    .start()

print("Streaming Gold is running...")
print("Writing station analytics to Gold Parquet...")
print("Writing customer analytics to Gold Parquet...")
print("Writing vehicle analytics to Gold Parquet...")

query.awaitTermination()
