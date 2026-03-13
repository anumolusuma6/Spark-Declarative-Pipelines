# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

dp.view
def customer_silver():
    spark.read_Stream.option("readChangeFeed", "true").table("my_projects_dev.cutsomers_silver.cutsomer_history")\
        .filter("__END_AT IS NULL")

dp.create_streaming_table
dp.create_auto_cdc_flow(
    target= "my_projects_dev.cutsomers_gold.cutsomer_gold",
    source= "customer_silver",
    key= ["id"],
    sequence_by= ["timestamp"],
    except_column_list= ["__START_AT", "__END_AT"],
    stored_as_scd_type=1
)
