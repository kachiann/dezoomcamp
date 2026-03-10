## Module 6 – Batch Processing with Spark (Homework)

Repository for my solutions to **Module 6: Batch Processing with Spark** of the Data Engineering Zoomcamp 2026 cohort.

---

### Setup

Environment:

- Devcontainer (Linux, VS Code)
- Java: OpenJDK 17 (forced via `JAVA_HOME` and `PATH`)
- PySpark: 4.1.1 (bundled Spark)
- Spark master: `local[*]`

Example Spark session:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dezoomcamp-hw6") \
    .getOrCreate()

print("Spark version:", spark.version)
```

Output:

```text
Spark version: 4.1.1
```

---

### Question 1 – Install Spark and PySpark

> Install Spark, run PySpark, create a local Spark session, execute `spark.version`.

> What’s the output?

**Answer:** `4.1.1`  

---

### Question 2 – Yellow November 2025 (Repartition)

> Read November 2025 Yellow data into a Spark DataFrame.  
> Repartition to 4 partitions and save to Parquet.  
> What is the average size of the Parquet files (MB)?

Script:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6-q2") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df_repart = df.repartition(4)

df_repart.write.mode("overwrite").parquet("yellow_2025_11_repart_4")

spark.stop()
```

Shell check:

```bash
cd yellow_2025_11_repart_4
ls -lh *.parquet
```

Output (each file):

```text
-rw-r--r-- 1 codespace codespace 26M ... part-00000-...
-rw-r--r-- 1 codespace codespace 26M ... part-00001-...
-rw-r--r-- 1 codespace codespace 26M ... part-00002-...
-rw-r--r-- 1 codespace codespace 26M ... part-00003-...
```

**Average size:** ~26 MB → **Answer:** **25MB** (closest option).  

---

### Question 3 – Count records on 15 November

> How many taxi trips started on 2025‑11‑15?

Script:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6-q3") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

count_15 = df.filter(col("pickup_date") == "2025-11-15").count()

print("Trips on 2025-11-15:", count_15)

spark.stop()
```

Output:

```text
Trips on 2025-11-15: 162604
```

**Answer:** **162,604**  

---

### Question 4 – Longest trip duration (hours)

> What is the length of the longest trip in hours?

Script:

```python
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
              .collect()["max_trip_hours"]

print("Max trip duration (hours):", max_hours)

spark.stop()
```

Output:

```text
Max trip duration (hours): 90.64666666666666
```

Rounded to 1 decimal: **90.6**  

**Answer:** **90.6**  

---

### Question 5 – Spark UI port

> Spark’s User Interface (application dashboard) runs on which local port?

Default Spark driver UI port is **4040**.

**Answer:** **4040**  

---

### Question 6 – Least frequent pickup location zone

> Using the yellow trips and `taxi_zone_lookup.csv`, what is the name of the least frequent pickup Zone?

Download zones:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Script:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6-q6") \
    .getOrCreate()

trips = spark.read.parquet("yellow_tripdata_2025-11.parquet")
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

trips = trips.withColumn("PULocationID", col("PULocationID").cast("int"))
zones = zones.withColumn("LocationID", col("LocationID").cast("int"))

joined = trips.join(zones, trips.PULocationID == zones.LocationID, "left")

counts = (joined
          .groupBy("Zone")
          .agg(count("*").alias("trip_count"))
          .orderBy(col("trip_count").asc()))

counts.show(20, truncate=False)

least = counts.first()
print("Least frequent pickup Zone:", least["Zone"],
      "with", least["trip_count"], "trips")

spark.stop()
```

Excerpt of output:

```text
+---------------------------------------------+----------+
|Zone                                         |trip_count|
+---------------------------------------------+----------+
|Governor's Island/Ellis Island/Liberty Island|1         |
|Eltingville/Annadale/Prince's Bay            |1         |
|Arden Heights                                |1         |
|Port Richmond                                |3         |
|Rikers Island                                |4         |
|Rossville/Woodrow                            |4         |
|Great Kills                                  |4         |
|Green-Wood Cemetery                          |4         |
|Jamaica Bay                                  |5         |
...
Least frequent pickup Zone: Governor's Island/Ellis Island/Liberty Island with 1 trips
```

Among the answer options, this matches directly.

**Answer:** **Governor's Island/Ellis Island/Liberty Island**  

---

### Summary of Answers

| Question | Answer                                               |
|---------:|------------------------------------------------------|
| Q1       | `4.1.1`                                              |
| Q2       | 25MB                                                 |
| Q3       | 162,604                                              |
| Q4       | 90.6 hours                                           |
| Q5       | 4040                                                 |
| Q6       | Governor's Island/Ellis Island/Liberty Island        |

---
