# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from functools import reduce

# Create Spark session (if not already created)
spark = SparkSession.builder.appName("Silver to Gold").getOrCreate()


# COMMAND ----------

Prod_df = spark.read.format("delta").load("/mnt/databricks_blob/Silver/Products")
cust_df = spark.read.format("delta").load("/mnt/databricks_blob/Silver/Customer")
emp_df = spark.read.format("delta").load("/mnt/databricks_blob/Silver/Employees")
ship_df = spark.read.format("delta").load("/mnt/databricks_blob/Silver/Shipper")
cat_df = spark.read.format("delta").load("/mnt/databricks_blob/Silver/categories")
od_df = spark.read.format("delta").load("/mnt/databricks_blob/Silver/order_details")
order_df = spark.read.format("delta").load("/mnt/databricks_blob/Silver/orders")

# COMMAND ----------

od_df = od_df.withColumn("Total", col("Quantity") * col("UnitPrice") - (col("Quantity") * col("UnitPrice") * col("Discount")))
emp_df = emp_df.withColumn("EmployeeName", concat(col("FirstName"), lit(" "), col("LastName")))

# COMMAND ----------

joined_df = order_df.join(cust_df, order_df.CustomerID == cust_df.CustomerID, "inner")\
                .join(od_df, order_df.OrderID == od_df.OrderID, "inner")\
                .join(Prod_df, od_df.ProductID == Prod_df.ProductID, "inner")\
                .join(cat_df, Prod_df.CategoryID == cat_df.CategoryID, "inner")\
                .join(ship_df, order_df.ShipVia == ship_df.ShipperID, "inner")\
                .join(emp_df, order_df.EmployeeID == emp_df.EmployeeID, "inner")\
                .select(order_df.OrderID, 
                        order_df.OrderDate,
                        order_df.RequiredDate,
                        order_df.ShippedDate,
                        order_df.ShipVia,
                        order_df.Freight,
                        order_df.ShipName,
                        order_df.ShipAddress,
                        order_df.ShipCity,
                        order_df.ShipRegion,
                        order_df.ShipPostalCode, 
                        order_df.ShipCountry, 
                        cust_df.CustomerID, 
                        cust_df.CompanyName, 
                        cust_df.ContactName, 
                        cust_df.ContactTitle, 
                        cust_df.Address, 
                        cust_df.City, 
                        cust_df.Region, 
                        cust_df.PostalCode, 
                        cust_df.Country, 
                        cust_df.Phone, 
                        cust_df.Fax, 
                        od_df.ProductID, 
                        od_df.Quantity, 
                        od_df.UnitPrice, 
                        od_df.Discount, 
                        od_df.Total, 
                        Prod_df.ProductName, 
                        Prod_df.UnitsInStock, 
                        Prod_df.Discontinued, 
                        cat_df.CategoryName, 
                        cat_df.Description, 
                        ship_df.CompanyName.alias("Shipper"), 
                        ship_df.Phone.alias("Shipper_phone"), 
                        emp_df.EmployeeID, 
                        emp_df.EmployeeName)\
                .orderBy(order_df.OrderID)


# COMMAND ----------

tgt_name = "Consolidated_Data"

joined_df.write.format("delta").mode("overwrite").save(f"/mnt/databricks_blob/Gold/{tgt_name}")
spark.sql(f"drop table if exists gold.{tgt_name}")
joined_df.write.format("delta").mode("overwrite").saveAsTable(f"gold.{tgt_name}")

# COMMAND ----------


