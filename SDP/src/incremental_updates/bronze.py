# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

df = spark.createDataFrame([
        {"id": 1, "name": "Alice","city": "NYC", "timestamp": "2022-01-01"},
        {"id": 2, "name": "Bob", "city": "SFO", "timestamp": "2022-01-01"},
        {"id": 1, "name": "Alice Smith", "city": "TX", "timestamp": "2022-01-01"},
        {"id": 3, "name": "Charlie", "city": "NYC", "timestamp": "2022-01-01"}
    ], schema="id INT, name STRING, city STRING, timestamp STRING")

df.write.mode("append").saveAsTable("my_projects_dev.cutsomers_bronze.cutsomers")

# COMMAND ----------

# Add new day in append mode
