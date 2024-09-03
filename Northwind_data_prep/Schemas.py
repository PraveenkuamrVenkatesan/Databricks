# Databricks notebook source
from pyspark.sql.types import *

#tablenames
# Categories
# Customer
# Employees
# order_details
# orders
# Products
# Shipper


# Define schema for Bronze data (if known)
Categories_schema = StructType([
    StructField("CategoryID", IntegerType(), True),
    StructField("CategoryName", StringType(), True),
    StructField("Description", StringType(), True)
])
