# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, month, year

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ecommerce Product Trend Analyzer") \
    .getOrCreate()

# COMMAND ----------

# Load the table into a PySpark DataFrame
df = spark.table("online_retail_ii")
# Display first few rows
df.display()

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# COMMAND ----------

df_clean = df.dropna()

# COMMAND ----------

df_clean = df_clean.withColumn("InvoiceDate", to_date(col("InvoiceDate")))

# COMMAND ----------

df_clean = df_clean.filter((col("Quantity") > 0) & (col("Price") > 0))

# COMMAND ----------

df_clean = df_clean.withColumn("TotalPrice", col("Quantity") * col("Price"))

# COMMAND ----------

df_clean.display()

# COMMAND ----------

from pyspark.sql.functions import month, year, sum

# Group by year and month and sum TotalPrice
sales_by_month = df_clean.withColumn("Year", year("InvoiceDate")) \
                         .withColumn("Month", month("InvoiceDate")) \
                         .groupBy("Year", "Month") \
                         .agg(sum("TotalPrice").alias("MonthlySales")) \
                         .orderBy("Year", "Month")

sales_by_month.display()

# COMMAND ----------

# Top 10 selling products by total sales
top_products = df_clean.groupBy("Description") \
                       .agg(sum("TotalPrice").alias("TotalSales")) \
                       .orderBy(col("TotalSales").desc()) \
                       .limit(10)

top_products.display()

# COMMAND ----------

