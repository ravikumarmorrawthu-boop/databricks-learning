# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# Read Bronze table
df = spark.table("databrickslearning.dev.employee_raw")

# Silver transformation: remove duplicates and add processed timestamp
df_clean = (
    df.dropDuplicates(["emp_id"])
      .withColumn("processed_ts", current_timestamp())
)

# Write to Silver table
df_clean.write.mode("overwrite").saveAsTable("databrickslearning.dev.employee_clean")

print("Silver load completed")