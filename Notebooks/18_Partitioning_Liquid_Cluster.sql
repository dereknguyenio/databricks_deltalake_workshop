-- Databricks notebook source
-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_raw_path = f"{base_path}/{username}/people10m_raw"
-- MAGIC adls_p_path = f"{base_path}/{username}/people10m_partition"
-- MAGIC adls_np_path = f"{base_path}/{username}/people10m_nopartition"
-- MAGIC adls_lc_path = f"{base_path}/{username}/people10m_liquidcluster"
-- MAGIC
-- MAGIC dbutils.widgets.text("adls_p_path", adls_p_path)
-- MAGIC dbutils.widgets.text("adls_np_path", adls_np_path)
-- MAGIC dbutils.widgets.text("adls_lc_path", adls_lc_path)
-- MAGIC dbutils.widgets.text("adls_raw_path", adls_raw_path)

-- COMMAND ----------

CREATE OR REPLACE TABLE people10m_raw
USING DELTA
LOCATION '${adls_raw_path}'
AS SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

-- COMMAND ----------

Create or replace table people10m_p
using delta
LOCATION '${adls_p_path}'
partitioned by (Year_of_Birth)
as select *,date_format(birthDate,'y') as Year_of_Birth from people10m



-- COMMAND ----------

Create or replace table people10m_np
using delta
LOCATION '${adls_np_path}'
as Select *,date_format(birthDate,'y') as Year_of_Birth from people10m

-- COMMAND ----------

SELECT id, firstname, middlename, lastname, gender, Year_of_Birth from people10m_p
where Year_of_Birth = '1980'

-- COMMAND ----------

SELECT id, firstname, middlename, lastname, gender, Year_of_Birth from people10m_np
where Year_of_Birth = '1980'

-- COMMAND ----------

--Describe people10m
SELECT 
  year(current_date()) - Year_of_Birth AS Age, 
  min(salary), 
  max(salary), 
  count(1) AS total_pax 
FROM people10m_p
GROUP BY Year_of_Birth

-- COMMAND ----------

SELECT 
  year(current_date()) - Year_of_Birth AS Age, 
  min(salary), 
  max(salary), 
  count(1) AS total_pax 
FROM people10m_np
GROUP BY Year_of_Birth


-- COMMAND ----------

CREATE OR REPLACE TABLE people10m_lc
CLUSTER BY (Year_of_Birth)
LOCATION '${adls_lc_path}'
AS SELECT 
    *,
    date_format(birthDate,'y') AS Year_of_Birth
FROM people10m;


-- COMMAND ----------

DESCRIBE DETAIL people10m_lc;

-- COMMAND ----------

describe history people10m_lc;

-- COMMAND ----------

SELECT 
  year(current_date()) - Year_of_Birth AS Age, 
  min(salary), 
  max(salary), 
  count(1) AS total_pax 
FROM people10m_lc
GROUP BY Year_of_Birth

-- COMMAND ----------

OPTIMIZE people10m_lc;

-- COMMAND ----------

SELECT 
  year(current_date()) - Year_of_Birth AS Age, 
  min(salary), 
  max(salary), 
  count(1) AS total_pax 
FROM people10m_lc
GROUP BY Year_of_Birth

-- COMMAND ----------

--Drop table 

-- COMMAND ----------

--drop table people10m_np;
--drop table people10m_p;