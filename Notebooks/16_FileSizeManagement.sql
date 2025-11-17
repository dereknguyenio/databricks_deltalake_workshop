-- Databricks notebook source
-- MAGIC %md
-- MAGIC Set target file size for optimize

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/people10m_dataskipping"
-- MAGIC dbutils.widgets.text("adls_path", adls_path)

-- COMMAND ----------

ALTER TABLE people10m SET TBLPROPERTIES (targetFileSize = 1073741824 )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Optimize for write operations. Performs additional shuffle but optimizes file sizes

-- COMMAND ----------

ALTER TABLE people10m SET TBLPROPERTIES (optimizeWrite = true)

-- COMMAND ----------

