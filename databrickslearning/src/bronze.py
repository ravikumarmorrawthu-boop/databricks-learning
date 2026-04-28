# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("catalog", "databrickslearning")
dbutils.widgets.text("schema", "dev")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

target_table = f"{catalog}.{schema}.employee_raw"

data = [
    (1, "Ravi", 101, 50000),
    (2, "John", 102, 60000),
    (3, "Sara", 101, 55000),
    (4, "Mike", 103, 65000)
]

columns = ["emp_id", "name", "dept_id", "salary"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("ingestion_ts", current_timestamp())

df.write.mode("overwrite").saveAsTable(target_table)

print(f"Bronze load completed: {target_table}")