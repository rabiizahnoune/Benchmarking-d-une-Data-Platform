from pyspark.sql import SparkSession
import time
import pandas as pd

# Initialize Spark with Delta configurations
spark = SparkSession.builder \
    .appName("Spark Benchmark") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Queries 
queries = {
    "Q1": "SELECT MAX(value) as max_temp FROM temperatures",
    "Q2": "SELECT MIN(value) as min_temp FROM temperatures",
    "Q3": "SELECT AVG(value) as avg_temp FROM temperatures",
    "Q4": "SELECT COUNT(*) as hot_cities FROM temperatures WHERE value > 20",
    "Q5": "SELECT SUM(value) as sum_temp FROM temperatures"
}

# Benchmark
results = []
data_sizes = ["sf10", "sf50", "sf100"]
formats = ["parquet", "orc", "delta"]
base_path = "/app/data"

for size in data_sizes:
    for fmt in formats:
        for query_id, query_sql in queries.items():
            start_time = time.time()
            if fmt == "parquet":
                df = spark.read.parquet(f"{base_path}/{size}/parquet/data.parquet")
            elif fmt == "orc":
                df = spark.read.orc(f"{base_path}/{size}/orc/data.orc")
            elif fmt == "delta":
                df = spark.read.format("delta").load(f"{base_path}/{size}/delta/")
            df.createOrReplaceTempView("temperatures")
            spark.sql(query_sql).collect()
            end_time = time.time()
            results.append({
                "Tool": "Spark",
                "Format": fmt,
                "DataSize": size,
                "Query": query_id,
                "Time_ms": (end_time - start_time) * 1000
            })

# Save results
df_results = pd.DataFrame(results)
df_results.to_csv("app/data/spark_query_times.csv", index=False)
print("Spark results saved to results/spark_query_times.csv")

# Stop Spark
spark.stop()