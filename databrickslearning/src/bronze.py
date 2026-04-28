# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (1, "Ravi", 101, 50000),
    (2, "John", 102, 60000),
    (3, "Sara", 101, 55000),
    (4, "Mike", 103, 65000)
]

columns = ["emp_id", "name", "dept_id", "salary"]

df = spark.createDataFrame(data, columns)

# Add ingestion timestamp
df = df.withColumn("ingestion_ts", current_timestamp())

# Write to Bronze (DEV schema for now)
df.write.mode("overwrite").saveAsTable("databrickslearning.dev.employee_raw")

print("Bronze load completed")