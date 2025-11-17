-- Databricks notebook source
-- MAGIC %md
-- MAGIC deletedFileRetentionDuration controls the minimum age before which parquet files cant be delete by vaccum. Has a default value of 7 days. Setting it to 1 hour for this demo 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/people10m_optimized2"
-- MAGIC
-- MAGIC dbutils.widgets.text("adls_path", adls_path)
-- MAGIC

-- COMMAND ----------

-- These table properties control Delta Lake retention behavior

ALTER TABLE people10m_optimized2 SET TBLPROPERTIES (

  -- 1. How long Delta keeps "tombstones" (removeFile entries)
  -- This MUST be smaller than the VACUUM retention if you want early file deletion.
  'delta.deletedFileRetentionDuration' = 'interval 5 minutes',

  -- 2. How long Delta keeps old _delta_log JSON checkpoint history
  -- Without lowering this, time travel metadata still sticks around.
  'delta.logRetentionDuration' = 'interval 5 minutes'
);


-- COMMAND ----------

Describe History people10m_optimized2

-- COMMAND ----------

delete from people10m_optimized2 where salary < 60000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute vaccum after an hour

-- COMMAND ----------

Vacuum  people10m_optimized2 RETAIN 1 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Time travel doesn't work as Vacuum has cleared older parquet files

-- COMMAND ----------

Select count(*) from people10m_optimized2 Version as of 12

-- COMMAND ----------

Select count(*) from people10m_optimized2 ;--where salary < 60000

-- COMMAND ----------

VACUUM people10m_optimized2;

-- COMMAND ----------

select * from people10m_optimized2

-- COMMAND ----------

delete from people10m_optimized2 where gender = 'M';

-- COMMAND ----------

