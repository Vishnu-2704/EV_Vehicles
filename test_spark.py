from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FirstSparkTest") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Version:", spark.version)

data = [
    ("Vishnu", 21),
    ("Rahul", 22),
    ("Priya", 21)
]

df = spark.createDataFrame(data, ["name", "age"])

df.show()

spark.stop()
