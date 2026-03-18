# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver = spark.readStream.table("my_projects_dev.customers_silver.customer_history")\
    .filter("__END_AT IS NULL")\
    .select(F.col("id"), F.col("name"), F.col("city"))

def upsert_to_gold(df, batch_id):
    if not spark.catalog.tableExists("my_projects_dev.customers_gold.customers_foreachbatch"):
        df.write.format("delta").saveAsTable("my_projects_dev.customers_gold.customer_foreachbatch")
        return
    
    df.createOrReplaceTempView("updates")
    spark.sql(f"""
    MERGE INTO "my_projects_dev.customers_gold.customer_foreachbatch" t
    USING updates s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

query = silver.writeStream\
    .foreachBatch(upsert_to_gold)\
    .option("checkpointLocation", "/Volumes/my_projects_dev/customers_gold/foreachBatch")\
    .trigger(once=True)\
    .start()

query.awaitTermination()
