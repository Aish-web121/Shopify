# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS Ecomm
# MAGIC MANAGED LOCATION 'abfss://uc-catalog@stg.dfs.core.windows.net/ecomm-catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC  create schema if not exists ecomm.bronze;
# MAGIC  create schema if not exists ecomm.silver;
# MAGIC   create schema if not exists ecomm.gold;