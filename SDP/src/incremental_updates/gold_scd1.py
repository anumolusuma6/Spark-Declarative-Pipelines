# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.view
def customer_silver():
    return spark.readStream.option("readChangeFeed", "true").table("my_projects_dev.customers_silver.customer_history")\
        .filter("__END_AT IS NULL")

dlt.create_streaming_table ("customer_gold")
dlt.create_auto_cdc_flow(
    target= "customer_gold",
    source= "customer_silver",
    keys= ["id"],
    sequence_by= F.col("timestamp"),
    except_column_list= ["__START_AT", "__END_AT"],
    stored_as_scd_type=1
)
