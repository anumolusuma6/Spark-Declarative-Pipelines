# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
dp.view
def customers_bronze():
    spark.readStream.table("my_projects_dev.cutsomers_bronze.cutsomes")

dp.create_streaming_table(
    name="customer_history",
    commenet= "SCD2 for customers"
)

dp.create_auto_cdc_flow(
    target = "my_projects_dev.cutsomers_silver.cutsomer_history",
    source = "customers_bronze",
    key= ["id"],
    sequence_by= ["timestamp"],
    stored_as_scd_type = 2
)
