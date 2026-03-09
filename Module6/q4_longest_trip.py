from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6-q4") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df = df.withColumn(
    "trip_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) -
     unix_timestamp(col("tpep_pickup_datetime"))) / 3600.0
)

max_hours = df.selectExpr("max(trip_hours) AS max_trip_hours") \
              .collect()[0]["max_trip_hours"]

print("Max trip duration (hours):", max_hours)

spark.stop()
