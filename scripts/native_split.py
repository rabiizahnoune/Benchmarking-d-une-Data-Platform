import polars as pl
from pyspark.sql import SparkSession
from deltalake.writer import write_deltalake
from pathlib import Path
import shutil

# Paths
data_file = "/app/data/measurement.txt"
base_path = "/app/data/"

# Define scale factors
scale_factors = {
    "sf10": 10_000_000,
    "sf50": 50_000_000,
    "sf100": 100_000_000
}

# Initialize Spark
spark = SparkSession.builder \
    .appName("DataConversion") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Process each scale factor in chunks
chunk_size = 5_000_000  # 5M rows per chunk
max_rows = 100_000_000  # Load only 100M total

batched_reader = pl.read_csv_batched(
    data_file,
    has_header=False,
    new_columns=["raw"],
    batch_size=chunk_size,
    truncate_ragged_lines=True,
    ignore_errors=True
)

for sf, row_limit in scale_factors.items():
    print(f"\nProcessing {sf} ({row_limit} rows)...")
    rows_processed = 0
    temp_dir = Path(f"{base_path}/temp_{sf}")
    temp_dir.mkdir(exist_ok=True)
    chunk_files = []
    
    # Create directories
    for fmt in ["parquet", "delta", "orc"]:
        Path(f"{base_path}/{sf}/{fmt}").mkdir(parents=True, exist_ok=True)
    
    i = 0
    while rows_processed < row_limit and rows_processed < max_rows:
        batches = batched_reader.next_batches(1)
        if not batches:
            break
        
        chunk = batches[0]
        df = chunk.with_columns([
            pl.col("raw").str.split_exact(";", 1).struct.rename_fields(["station", "value"])
        ]).unnest("raw").with_columns([
            pl.col("value").cast(pl.Float64, strict=False)
        ])
        
        remaining = row_limit - rows_processed
        if len(df) > remaining:
            df = df.slice(0, remaining)
        
        # Write temp Parquet chunk
        chunk_file = f"{temp_dir}/chunk_{i}.parquet"
        df.write_parquet(chunk_file)
        chunk_files.append(chunk_file)
        
        # Convert to other formats
        pandas_df = df.to_pandas()
        spark_df = spark.createDataFrame(pandas_df)
        
        # Delta 
        mode = "append" if rows_processed > 0 else "overwrite"
        write_deltalake(f"{base_path}/{sf}/delta/", pandas_df, mode=mode)
        print(f"{sf} chunk saved as Delta")
        
        # ORC 
        spark_df.repartition(10).write.orc(f"{base_path}/{sf}/orc/data.orc", mode=mode)
        print(f"{sf} chunk saved as ORC")
        
        rows_processed += len(df)
        print(f"Processed {rows_processed} rows for {sf}")
        i += 1
    
    # Combine Parquet chunks
    if chunk_files:
        combined_df = pl.concat([pl.read_parquet(f) for f in chunk_files])
        combined_df.write_parquet(f"{base_path}/{sf}/parquet/data.parquet")
        print(f"{sf} saved as Parquet")
        shutil.rmtree(temp_dir)  # Clean up
    
    print(f"{sf} completed with {rows_processed} rows")

spark.stop()
print("\nDone! All splits saved in all formats!")