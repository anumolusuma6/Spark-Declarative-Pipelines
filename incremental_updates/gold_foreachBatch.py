# Databricks notebook source
silver = spark.read_Stream.table("my_projects_dev.cutsomers_silver.cutsomer_history")\
    .filter("__END_AT IS NULL")\
    .select(F.col("id"), F.col("name"), F.col("city"))

def upsert_to_gold(df, batch_id):
    if not spark.catalog.tableExists("my_projects_dev.cutsomers_silver.cutsomer_gold"):
        df.write("my_projects_dev.cutsomers_silver.cutsomer_gold")
        return
    
    df.createOrReplaceTempView("updates")
    spark.sql(f"""
    MERGE INTO "cavallo_dev.default.customers_gold" t
    USING updates s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

query = silver.writeStream\
    .foreachBatch(upsert_to_gold)\
    .option("checkpointLocation", "/Volumes/my_projects_dev/customer_gold/foreachBatch")\
    .trigger(once=True)\
    .start()

query.awaitTermination()