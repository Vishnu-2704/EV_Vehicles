from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    sum,
    count,
    avg
)
from pathlib import Path

# -----------------------------------
# 1. Project paths
# -----------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

input_path = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "charging_transactions"
)

output_root = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "gold"
)

daily_output = output_root / "daily_analytics"
station_output = output_root / "station_analytics"
customer_output = output_root / "customer_analytics"


print("========================================")
print("EV GOLD ANALYTICS")
print("========================================")


# -----------------------------------
# 2. Start Spark
# -----------------------------------

spark = SparkSession.builder \
    .appName("EV Gold Analytics") \
    .master("local[*]") \
    .getOrCreate()

print("\nSpark started!")
print("Spark version:", spark.version)


# -----------------------------------
# 3. Read Silver
# -----------------------------------

print("\nReading Silver data...")

df = spark.read.parquet(str(input_path))

print("Silver data loaded!")

silver_count = df.count()

print("Silver Records:", silver_count)


# -----------------------------------
# 4. Add charging date
# -----------------------------------

print("\nCreating charging date...")

df = df.withColumn(
    "charging_date",
    to_date(col("start_time"))
)

print("Charging date created.")


# -----------------------------------
# 5. Daily Analytics
# -----------------------------------

print("\nCreating Daily Analytics...")

daily_df = df.groupBy("charging_date").agg(
    count("transaction_id").alias("total_sessions"),
    sum("energy_kwh").alias("total_energy_kwh"),
    sum("amount_paid").alias("total_revenue"),
    avg("energy_kwh").alias("average_energy_kwh")
)

daily_df = daily_df.orderBy("charging_date")

print("\nDaily Analytics:")

daily_df.show(10, truncate=False)


# -----------------------------------
# 6. Station Analytics
# -----------------------------------

print("\nCreating Station Analytics...")

station_df = df.groupBy("station_id").agg(
    count("transaction_id").alias("total_sessions"),
    sum("energy_kwh").alias("total_energy_kwh"),
    sum("amount_paid").alias("total_revenue"),
    avg("energy_kwh").alias("average_energy_kwh")
)

station_df = station_df.orderBy(
    col("total_energy_kwh").desc()
)

print("\nStation Analytics:")

station_df.show(10, truncate=False)


# -----------------------------------
# 7. Customer Analytics
# -----------------------------------

print("\nCreating Customer Analytics...")

customer_df = df.groupBy("customer_id").agg(
    count("transaction_id").alias("total_sessions"),
    sum("energy_kwh").alias("total_energy_kwh"),
    sum("amount_paid").alias("total_spending"),
    avg("energy_kwh").alias("average_energy_kwh")
)

customer_df = customer_df.orderBy(
    col("total_spending").desc()
)

print("\nCustomer Analytics:")

customer_df.show(10, truncate=False)


# -----------------------------------
# 8. Create Gold folders
# -----------------------------------

daily_output.parent.mkdir(
    parents=True,
    exist_ok=True
)

station_output.parent.mkdir(
    parents=True,
    exist_ok=True
)

customer_output.parent.mkdir(
    parents=True,
    exist_ok=True
)


# -----------------------------------
# 9. Write Gold datasets
# -----------------------------------

print("\nWriting Daily Analytics...")

daily_df.write \
    .mode("overwrite") \
    .parquet(str(daily_output))


print("Writing Station Analytics...")

station_df.write \
    .mode("overwrite") \
    .parquet(str(station_output))


print("Writing Customer Analytics...")

customer_df.write \
    .mode("overwrite") \
    .parquet(str(customer_output))


print("\n*** GOLD WRITE FINISHED ***")


# -----------------------------------
# 10. Verify Gold
# -----------------------------------

print("\nVerifying Gold datasets...")

daily_count = spark.read.parquet(
    str(daily_output)
).count()

station_count = spark.read.parquet(
    str(station_output)
).count()

customer_count = spark.read.parquet(
    str(customer_output)
).count()

print("Daily Analytics Records:", daily_count)
print("Station Analytics Records:", station_count)
print("Customer Analytics Records:", customer_count)


# -----------------------------------
# 11. Stop Spark
# -----------------------------------

spark.stop()

print("\n========================================")
print("GOLD PIPELINE COMPLETED")
print("========================================")
