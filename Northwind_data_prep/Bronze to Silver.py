# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from functools import reduce



# Initialize Spark session (this is typically handled by Databricks automatically)
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Define paths for Bronze and Silver layers
bronze_path = "/mnt/databricks_blob/Bronze/"
silver_path = "/mnt/databricks_blob/Silver/"

# # Define schema for Bronze data (if known)
# bronze_schema = StructType([
#     StructField("column1", StringType(), True),
#     StructField("column2", IntegerType(), True),
#     StructField("column3", TimestampType(), True),
#     # Add more fields as per your data
# ])

# # # Step 1: Read data from Bronze layer
# bronze_df = spark.read.parquet(bronze_path)

files = dbutils.fs.ls(bronze_path)

for file in files:
    file_path = file.path
    tgt_name = file.name
    print(f"Processing file: {file_path}")
    
    # Read each Parquet file into a DataFrame
    bronze_df = spark.read.parquet(file_path)
    # Step 2: Data Cleaning and Transformation
    # Example: Remove duplicates, filter out bad data, add new columns, etc.

    # Remove duplicates
    bronze_df = bronze_df.dropDuplicates()
    bronze_df.cache()
    bronze_df.count()
    bronze_df.persist()

    condition = reduce(lambda x, y: x & y, (col(c).isNotNull() for c in bronze_df.columns), lit(True))

    # Filter out rows with nulls in critical columns
    bronze_df = bronze_df.filter(condition)

    # Example transformation: Add a new column with the current processing timestamp
    bronze_df = bronze_df.withColumn("processed_at", current_timestamp())

    # Step 3: Write the cleaned data to the Silver layer
    bronze_df.write.format("delta").mode("overwrite").save(f"/mnt/databricks_blob/Silver/{tgt_name}")
    spark.sql(f"drop table if exists Silver.{tgt_name}")
    bronze_df.write.format("delta").mode("overwrite").saveAsTable(f"Silver.{tgt_name}")
    bronze_df.unpersist()

    # # Optionally, create a table in Databricks from the Silver data for further analysis
    # spark.sql("""
    #     CREATE TABLE IF NOT EXISTS silver.{}
    #     USING Delta
    #     LOCATION '{}'
    # """.format(tgt_name,silver_path))



    # Optionally, save a snapshot of the Silver layer as a backup
    # backup_path = "/mnt/silver-layer-backup/"
    # silver_df.write.mode("overwrite").parquet(backup_path)

