from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6-q6") \
    .getOrCreate()

trips = spark.read.parquet("yellow_tripdata_2025-11.parquet")

zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

# Standard NYC schema: PULocationID is integer key into zones.LocationID
trips = trips.withColumn("PULocationID", col("PULocationID").cast("int"))
zones = zones.withColumn("LocationID", col("LocationID").cast("int"))

joined = trips.join(zones, trips.PULocationID == zones.LocationID, "left")

counts = (joined
          .groupBy("Zone")
          .agg(count("*").alias("trip_count"))
          .orderBy(col("trip_count").asc()))

counts.show(20, truncate=False)

least = counts.first()
print("Least frequent pickup Zone:", least["Zone"], "with", least["trip_count"], "trips")

spark.stop()
