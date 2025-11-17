-- Databricks notebook source
-- MAGIC %md
-- MAGIC Create a table to test cloning options in delta table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/clone_test"
-- MAGIC adls_sc_path = f"{base_path}/{username}/ShallowClone"
-- MAGIC adls_dc_path = f"{base_path}/{username}/DeepClone"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""Create or replace table clone_test 
-- MAGIC           using Delta location '{adls_path}' 
-- MAGIC           as Select * from people10m""")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Shallow clone example - happens quickly

-- COMMAND ----------

select * from clone_test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""create or replace table clone_test_sc SHALLOW CLONE clone_test location '{adls_sc_path}'""")
-- MAGIC  #Drop table clone_test_sc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Files would reference old path

-- COMMAND ----------

--select distinct input_file_name() from clone_test_sc;

select distinct _metadata.file_path from clone_test_sc;

-- COMMAND ----------

select count(*) from clone_test_sc

-- COMMAND ----------

Insert into clone_test_sc select * from clone_test

-- COMMAND ----------

select count(*) from clone_test_sc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Files would show both old and new path as Insert statement added new parquet files

-- COMMAND ----------

--select distinct input_file_name() from clone_test_sc;

select distinct _metadata.file_path from clone_test_sc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC making changes at original table would not affect clone table

-- COMMAND ----------

Insert into clone_test select * from clone_test

-- COMMAND ----------

select count(*) from clone_test_sc

-- COMMAND ----------

--select distinct input_file_name() from clone_test_sc;

select distinct _metadata.file_path from clone_test_sc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC deep clone physically copies the files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""create or replace table clone_test_dc  CLONE clone_test_sc location '{adls_dc_path}'""")

-- COMMAND ----------

--Select distinct  input_file_name() from clone_test_dc

select distinct _metadata.file_path from clone_test_dc;

-- COMMAND ----------

Insert into clone_test_dc Select * from clone_test

-- COMMAND ----------

Select count(*) from clone_test_dc

-- COMMAND ----------

--Select distinct  input_file_name() from ;

select distinct _metadata.file_path from clone_test_sc;