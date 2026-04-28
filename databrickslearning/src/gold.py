# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, current_timestamp

spark = SparkSession.builder.getOrCreate()

# Read Silver table
df = spark.table("databrickslearning.dev.employee_clean")

# Gold aggregation
df_gold = (
    df.groupBy("dept_id")
      .agg(
          count("emp_id").alias("employee_count"),
          sum("salary").alias("total_salary"),
          avg("salary").alias("avg_salary")
      )
      .withColumn("gold_processed_ts", current_timestamp())
)

# Write to Gold table
df_gold.write.mode("overwrite").saveAsTable("databrickslearning.dev.department_salary_summary")

print("Gold load completed")