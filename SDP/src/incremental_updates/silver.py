# Databricks notebook source
# DBTITLE 1,Cell 1
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.view
def bronze():
    return spark.readStream.table("my_projects_dev.customers_bronze.customers")

dlt.create_streaming_table ("customer_history")
dlt.create_auto_cdc_flow(
    target = "customer_history",
    source = "bronze",
    keys= ["id"],
    sequence_by= F.col("timestamp"),
    stored_as_scd_type = 2
)
