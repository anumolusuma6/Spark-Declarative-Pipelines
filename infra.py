# Databricks notebook source
spark.sql("CREATE CATALOG IF NOT EXISTS my_projects_dev")
spark.sql("CREATE SCHEMA IF NOT EXISTS my_projects_dev.customers_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS my_projects_dev.customers_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS my_projects_dev.customers_gold")
spark.sql("CREATE VOLUME IF NOT EXISTS my_projects_dev.customers_gold.foreachBatch")