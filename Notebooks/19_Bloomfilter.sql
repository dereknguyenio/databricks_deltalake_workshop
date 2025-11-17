-- Databricks notebook source
-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/people10m_indexed"
-- MAGIC
-- MAGIC dbutils.widgets.text("adls_path", adls_path)
-- MAGIC

-- COMMAND ----------

--Drop table people10m_indexed;
Create table people10m_indexed
using delta
location '${adls_path}'
as
select * from people10m

-- COMMAND ----------

SET spark.databricks.io.skipping.bloomFilter.enabled = true;

-- COMMAND ----------

CREATE BLOOMFILTER INDEX
ON TABLE people10m_indexed
FOR COLUMNS(firstName OPTIONS (fpp=0.1, numItems=50000))

-- COMMAND ----------

Optimize people10m_indexed Zorder by (firstName)

-- COMMAND ----------

Select count(*) from  people10m_indexed
where firstName = "Terrie"



-- COMMAND ----------

Select count(*),LastName from  people10m where firstName = "Nagaraj" 
Group by LastName

-- COMMAND ----------

Select count(*) from  people10m_indexed
where  LastName = "McGrirl"


-- COMMAND ----------

Select * from  people10m_indexed