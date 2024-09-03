# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from functools import reduce
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("MyExercise1") \
    .getOrCreate()




# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/exercise/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Load the supermarket_invoice_data.csv file into a PySpark DataFrame and display the first 5 rows of the DataFrame.**

# COMMAND ----------

#Paste your Answer here
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/tables/exercise/supermarket_invoice_data.csv")
display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Filter all invoices where the payment method is "Credit Card" and retrieve all invoices where the total price is greater than $500.**

# COMMAND ----------

#Paste your Answer here
filter_df = df.where("Payment_method = 'Credit Card' and Total_Price > 500")
display(filter_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Calculate the total revenue generated by each store and find the average unit price of products sold in each store.**

# COMMAND ----------

#Paste your Answer here
Total_revenue = df.groupby("Store_ID").agg(sum(col("Total_price")).alias("Total_revenue"))
avg_prod_price = df.groupby("Store_ID","Product_ID").agg(avg(col("Unit_Price")).alias("Avg_product_price"))

display(Total_revenue.join(avg_prod_price, how="inner", on="Store_ID").select("Store_ID","Product_ID","Total_revenue","Avg_product_price").orderBy("Store_ID","Product_ID"))

# COMMAND ----------

total_revenue = df.withColumn("Total_revenue", sum(col("Total_price")).over(Window.partitionBy("Store_ID")))\
                .withColumn("Avg_product_price", avg(col("Unit_Price")).over(Window.partitionBy("Store_ID","Product_ID")))\
                .select("Store_ID","Product_ID","Total_revenue","Avg_product_price")\
                .orderBy("Store_ID","Product_ID").distinct()

display(total_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC **4. a)Group the data by Customer_ID and calculate the total quantity purchased by each customer.**
# MAGIC
# MAGIC **b)Group the data by Product_ID and find the maximum and minimum Total_Price for each product.**

# COMMAND ----------

#Paste your Answer here
#a)
total_qty = df.groupBy("Customer_ID").agg(sum(col("Quantity")).alias("Total_quantity_purchased"))
display(total_qty)

# COMMAND ----------

#b)
min_max_price = df.groupBy("Product_ID").agg(min(col("Total_price")).alias("Min_price"), max(col("Total_price")).alias("Max_price"))

display(min_max_price)

# COMMAND ----------

# MAGIC %md
# MAGIC **5.a) Use a window function to calculate the cumulative total price for each customer b)Rank products based on their total sales in descending order.**

# COMMAND ----------

#Paste your Answer here
#a)
cum_df = df.withColumn("Cumulative_total_price", sum(col("Total_price")).over(Window.partitionBy("Customer_ID").orderBy("Invoice_Date")))\
    .select("Customer_ID", "Invoice_Date", "Total_price", "Cumulative_total_price")

display(cum_df)

# COMMAND ----------

#b)

r_prod = df.groupBy("Product_ID").agg(sum(col("Total_price")).alias("Prod_total"))\
            .withColumn("rank", rank().over(Window.orderBy(desc("Prod_total"))))

display(r_prod)

# COMMAND ----------

# MAGIC %md
# MAGIC **6. Extract the year and month from the Invoice_Date column and calculate the number of days since the invoice date for each row.**

# COMMAND ----------

#Paste your Answer here
day_diff_df = df.withColumn("month", month(col("Invoice_Date")))\
                .withColumn("year", year(col("Invoice_Date")))\
                .withColumn("diff", date_diff(Col("Invoice_Date"), lead("Invoice_Date").over(Window.partitionBy("Customer_ID").orderBy("Invoice_Date")))\
                .select("Customer_ID", "month","year", "diff")

display(day_diff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **7. Convert the Total_Price from USD to IND using a conversion rate of 1 USD = 83 IND.**

# COMMAND ----------

#Paste your Answer here

# COMMAND ----------

# MAGIC %md
# MAGIC **8. Pivot the invoice data to show the total quantity purchased by each customer for each product.**

# COMMAND ----------



# COMMAND ----------

#Paste your Answer here

# COMMAND ----------

# MAGIC %md
# MAGIC **9. Calculate the total sales and average unit price for each payment method.**

# COMMAND ----------

#Paste your Answer here

# COMMAND ----------

# MAGIC %md
# MAGIC **10. Find out which product is the most popular based on the total quantity sold.**

# COMMAND ----------

#Paste your Answer here

# COMMAND ----------

spark.stop()
