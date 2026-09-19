from pyspark.sql import SparkSession
from pathlib import Path
import os

# ==========================================
# 1. Project paths
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

input_path = PROJECT_ROOT / "data" / "raw" / "charging_transactions.csv"
output_path = PROJECT_ROOT / "data" / "processed" / "bronze" / "charging_transactions"

# Convert Windows path to file URI for Spark
input_uri = input_path.as_uri()
output_uri = output_path.as_uri()

print("========================================")
print("EV BRONZE INGESTION")
print("========================================")

print("Project root:", PROJECT_ROOT)
print("Input:", input_path)
print("Output:", output_path)

# ==========================================
# 2. Check input
# ==========================================

if not input_path.exists():
    raise FileNotFoundError(f"Input file not found: {input_path}")

print("\nInput file exists!")

# ==========================================
# 3. Start Spark
# ==========================================

spark = SparkSession.builder \
    .appName("EV Bronze Ingestion") \
    .master("local[*]") \
    .getOrCreate()

print("\nSpark version:", spark.version)

# ==========================================
# 4. Read CSV
# ==========================================

print("\nReading CSV...")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_uri)

print("CSV loaded!")

# ==========================================
# 5. Check data
# ==========================================

print("\nSchema:")
df.printSchema()

count = df.count()

print("\nTotal Records:", count)

print("\nSample:")
df.show(5, truncate=False)

# ==========================================
# 6. Create output parent folder
# ==========================================

output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)

# Remove old output if Python can see it
if output_path.exists():
    import shutil
    shutil.rmtree(output_path)

print("\nWriting Bronze Parquet...")

# ==========================================
# 7. Write Parquet
# ==========================================

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(output_uri)

print("\n*** BRONZE WRITE FINISHED ***")

# ==========================================
# 8. Verify using Spark itself
# ==========================================

print("\nVerifying Bronze data using Spark...")

bronze_df = spark.read.parquet(output_uri)

bronze_count = bronze_df.count()

print("Bronze Records:", bronze_count)

if bronze_count == count:
    print("\nSUCCESS!")
    print("Bronze contains all", bronze_count, "records.")
else:
    print("\nERROR!")
    print("Input records:", count)
    print("Bronze records:", bronze_count)

# ==========================================
# 9. Show files using Python
# ==========================================

print("\nFiles physically present:")

if output_path.exists():

    for root, dirs, files in os.walk(output_path):
        for file in files:
            print(" ->", Path(root) / file)

else:
    print("ERROR: Bronze folder does not exist!")

# ==========================================
# 10. Stop Spark
# ==========================================

spark.stop()

print("\n========================================")
print("BRONZE PIPELINE COMPLETED")
print("========================================")
