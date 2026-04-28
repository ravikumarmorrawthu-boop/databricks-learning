# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, current_timestamp

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("catalog", "databrickslearning")
dbutils.widgets.text("schema", "dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = f"{catalog}.{schema}.employee_clean"
target_table = f"{catalog}.{schema}.department_salary_summary"

df = spark.table(source_table)

df_gold = (
    df.groupBy("dept_id")
      .agg(
          count("emp_id").alias("employee_count"),
          sum("salary").alias("total_salary"),
          avg("salary").alias("avg_salary")
      )
      .withColumn("gold_processed_ts", current_timestamp())
)

df_gold.write.mode("overwrite").saveAsTable(target_table)

print(f"Gold load completed: {target_table}")