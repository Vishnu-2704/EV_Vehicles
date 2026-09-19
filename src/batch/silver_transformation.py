from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pathlib import Path

# -----------------------------------
# 1. Find project root
# -----------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

input_path = PROJECT_ROOT / "data" / "processed" / "bronze" / "charging_transactions"
output_path = PROJECT_ROOT / "data" / "processed" / "silver" / "charging_transactions"

print("========================================")
print("EV SILVER TRANSFORMATION")
print("========================================")

print("Project root:")
print(PROJECT_ROOT)

print("\nBronze input:")
print(input_path)

print("\nSilver output:")
print(output_path)


# -----------------------------------
# 2. Start Spark
# -----------------------------------

spark = SparkSession.builder \
    .appName("EV Silver Transformation") \
    .master("local[*]") \
    .getOrCreate()

print("\nSpark started!")
print("Spark version:", spark.version)


# -----------------------------------
# 3. Read Bronze Parquet
# -----------------------------------

print("\nReading Bronze data...")

df = spark.read.parquet(str(input_path))

print("Bronze data loaded!")

print("\nBronze record count:")
print(df.count())


# -----------------------------------
# 4. Clean string columns
# -----------------------------------

print("\nCleaning string columns...")

df_clean = df \
    .withColumn("transaction_id", trim(col("transaction_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("vehicle_id", trim(col("vehicle_id"))) \
    .withColumn("station_id", trim(col("station_id"))) \
    .withColumn("status", trim(col("status")))

print("String cleaning completed.")


# -----------------------------------
# 5. Remove NULL IDs
# -----------------------------------

print("\nRemoving records with NULL IDs...")

df_clean = df_clean.filter(
    col("transaction_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("vehicle_id").isNotNull() &
    col("station_id").isNotNull()
)

print("NULL ID validation completed.")


# -----------------------------------
# 6. Validate energy
# -----------------------------------

print("\nValidating energy consumption...")

df_clean = df_clean.filter(
    col("energy_kwh") > 0
)

print("Energy validation completed.")


# -----------------------------------
# 7. Validate payment
# -----------------------------------

print("\nValidating payment amount...")

df_clean = df_clean.filter(
    col("amount_paid") >= 0
)

print("Payment validation completed.")


# -----------------------------------
# 8. Validate timestamps
# -----------------------------------

print("\nValidating timestamps...")

df_clean = df_clean.filter(
    col("start_time").isNotNull() &
    col("end_time").isNotNull() &
    (col("end_time") >= col("start_time"))
)

print("Timestamp validation completed.")


# -----------------------------------
# 9. Remove duplicate transactions
# -----------------------------------

print("\nRemoving duplicate transactions...")

df_clean = df_clean.dropDuplicates(["transaction_id"])

print("Duplicate removal completed.")


# -----------------------------------
# 10. Count Silver records
# -----------------------------------

silver_count = df_clean.count()

print("\n========================================")
print("SILVER RECORD COUNT:", silver_count)
print("========================================")


# -----------------------------------
# 11. Display Silver sample
# -----------------------------------

print("\nSilver sample:")

df_clean.show(5, truncate=False)


# -----------------------------------
# 12. Create Silver directory
# -----------------------------------

output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)


# -----------------------------------
# 13. Write Silver Parquet
# -----------------------------------

print("\nWriting Silver Parquet...")

df_clean.write \
    .mode("overwrite") \
    .parquet(str(output_path))

print("\n*** SILVER WRITE FINISHED ***")


# -----------------------------------
# 14. Verify Silver data
# -----------------------------------

print("\nVerifying Silver data...")

verify_df = spark.read.parquet(str(output_path))

verify_count = verify_df.count()

print("Silver Records:", verify_count)

if verify_count == silver_count:
    print("\nSUCCESS!")
    print("Silver data verification passed.")

else:
    print("\nERROR!")
    print("Silver verification failed.")


# -----------------------------------
# 15. Stop Spark
# -----------------------------------

spark.stop()

print("\n========================================")
print("SILVER PIPELINE COMPLETED")
print("========================================")
