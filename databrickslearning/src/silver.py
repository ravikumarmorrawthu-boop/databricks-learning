# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("catalog", "databrickslearning")
dbutils.widgets.text("schema", "dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = f"{catalog}.{schema}.employee_raw"
target_table = f"{catalog}.{schema}.employee_clean"

df = spark.table(source_table)

df_clean = (
    df.dropDuplicates(["emp_id"])
      .withColumn("processed_ts", current_timestamp())
)

df_clean.write.mode("overwrite").saveAsTable(target_table)

print(f"Silver load completed: {target_table}")