# NYC Taxi Data Analysis using PySpark

## 📘 Objective:
Load NYC Yellow Taxi Trip Data into Azure Data Lake / Blob Storage / Databricks, extract it into a PySpark DataFrame, and perform multiple PySpark queries.

---

## 📂 Dataset Source

| Dataset                        | Format  | Link                                                        |
|-------------------------------|---------|-------------------------------------------------------------|
| Yellow Taxi Trip Data – Jan 2018 | Parquet | [yellow_tripdata_2018-01.parquet](https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-01.parquet) |

---

## 🚀 Step 1: Setup & Load Dataset

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName("NYC Taxi Data Analysis")     .getOrCreate()

df = spark.read.option("header", "true")     .option("inferSchema", "true")     .parquet("/mnt/nyc-taxi/yellow_tripdata_2018-01")
```

---

## 🔎 Step 2: Queries

### ✅ Query 1: Add a Revenue Column

```python
from pyspark.sql.functions import col

df = df.withColumn("Revenue",
    col("fare_amount") + col("extra") + col("mta_tax") +
    col("improvement_surcharge") + col("tip_amount") +
    col("tolls_amount") + col("total_amount")
)

df.select("Revenue").show(5)
```

---

### ✅ Query 2: Total Passengers by Pickup Area

```python
df.groupBy("PULocationID")   .sum("passenger_count")   .withColumnRenamed("sum(passenger_count)", "total_passengers")   .orderBy("total_passengers", ascending=True)   .show()
```

---

### ✅ Query 3: Average Fare / Total Earnings by Vendor

```python
df.groupBy("VendorID")   .avg("fare_amount", "total_amount")   .withColumnRenamed("avg(fare_amount)", "average_fare")   .withColumnRenamed("avg(total_amount)", "average_earning")   .show()
```

---

### ✅ Query 4: Moving Count of Payments by Each Payment Mode

```python
from pyspark.sql.functions import to_timestamp, count
from pyspark.sql.window import Window

df = df.withColumn("pickup_time", to_timestamp("tpep_pickup_datetime"))

windowSpec = Window.partitionBy("payment_type")                    .orderBy("pickup_time")                    .rowsBetween(-10, 0)

df_with_moving_count = df.withColumn("Moving_Payment_Count", count("*").over(windowSpec))

df_with_moving_count.select("pickup_time", "payment_type", "Moving_Payment_Count").show()
```

---

### ✅ Query 5: Highest Two Gaining Vendors on a Particular Date

```python
from pyspark.sql.functions import to_date, sum as _sum

df = df.withColumn("date", to_date("tpep_pickup_datetime"))

df_filtered = df.filter(df["date"] == "2018-01-15")

top_vendors = df_filtered.groupBy("VendorID")     .agg(
        _sum("Revenue").alias("total_revenue"),
        _sum("passenger_count").alias("total_passengers"),
        _sum("trip_distance").alias("total_distance")
    )     .orderBy("total_revenue", ascending=False)     .limit(2)

top_vendors.show()
```

---

### ✅ Query 6: Route with Most Passengers

```python
df.groupBy("PULocationID", "DOLocationID")   .sum("passenger_count")   .withColumnRenamed("sum(passenger_count)", "total_passengers")   .orderBy("total_passengers", ascending=False)   .show(1)
```

---

### ✅ Query 7: Top Pickup Locations with Most Passengers in Last 10 Seconds

```python
from pyspark.sql.functions import max as _max, expr

latest_time = df.agg(_max("pickup_time")).first()[0]

df_recent = df.filter(expr(f"pickup_time > timestamp('{latest_time}') - interval 10 seconds"))

df_recent.groupBy("PULocationID")   .sum("passenger_count")   .withColumnRenamed("sum(passenger_count)", "recent_passengers")   .orderBy("recent_passengers", ascending=False)   .show()
```

---

## 📌 Notes
- Run all queries in Databricks or PySpark environment.
- Dataset must be loaded as a DataFrame before executing queries.
- Ensure correct timestamp format for time-based queries.
