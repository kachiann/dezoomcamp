from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6-q3") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# Adjust column name if needed; in NYC data it's usually tpep_pickup_datetime
df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

count_15 = df.filter(col("pickup_date") == "2025-11-15").count()

print("Trips on 2025-11-15:", count_15)

spark.stop()
