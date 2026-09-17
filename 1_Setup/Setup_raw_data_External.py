# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog ecomm;
# MAGIC create schema if not exists raw;
# MAGIC
# MAGIC create external volume if not exists raw.raw_landing
# MAGIC location 'abfss://ecomm@stg.dfs.core.windows.net/';