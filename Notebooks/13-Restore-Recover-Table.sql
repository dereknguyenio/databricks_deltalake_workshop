-- Databricks notebook source
-- MAGIC %md
-- MAGIC Revert a table to a version

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/people10m_table"
-- MAGIC dbutils.widgets.text("adls_path", adls_path)

-- COMMAND ----------

Describe history people10m

-- COMMAND ----------

RESTORE TABLE people10m TO VERSION AS OF 5
--RESTORE TABLE delta.`/data/target/` TO TIMESTAMP AS OF <timestamp>

-- COMMAND ----------

Describe detail people10m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop table but recover before hard delete from storage

-- COMMAND ----------

Drop table people10m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Go to Azure Data Lake, use soft delete - revert feature to bring the parquet and delta log files

-- COMMAND ----------

CREATE TABLE people10m 
using delta
Location '${adls_path}'

-- COMMAND ----------

Select count(*) from people10m 

-- COMMAND ----------

Describe history people10m 

-- COMMAND ----------

