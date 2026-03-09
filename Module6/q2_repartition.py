from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6-q2") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df_repart = df.repartition(4)

df_repart.write.mode("overwrite").parquet("yellow_2025_11_repart_4")

spark.stop()
