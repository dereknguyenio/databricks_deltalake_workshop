-- Databricks notebook source
-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/people10m_dataskipping"
-- MAGIC dbutils.widgets.text("adls_path", adls_path)

-- COMMAND ----------

Create or replace table people10m_dataskipping
using delta
location '${adls_path}'
as
Select * from people10m_stg

-- COMMAND ----------

Describe  people10m_dataskipping

-- COMMAND ----------

-- MAGIC %md
-- MAGIC increase data skipping to 35 columns

-- COMMAND ----------

ALTER TABLE people10m_dataskipping SET TBLPROPERTIES (delta.dataSkippingNumIndexedCols = 35)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC changing column order to bring the column into first 32 which enables data skipping

-- COMMAND ----------

ALTER TABLE people10m_dataskipping CHANGE COLUMN SSN AFTER id

-- COMMAND ----------

Describe people10m_dataskipping

-- COMMAND ----------

