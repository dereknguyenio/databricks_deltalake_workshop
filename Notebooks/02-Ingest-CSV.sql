-- Databricks notebook source
-- MAGIC %md
-- MAGIC use scala to read csv files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/diamonds"
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.format("csv") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .option("inferSchema", "true") \
-- MAGIC   .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
-- MAGIC
-- MAGIC display(diamonds)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create delta table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Now recreate it externally by specifying the path
-- MAGIC diamonds.write \
-- MAGIC     .format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .saveAsTable("diamonds")
-- MAGIC
-- MAGIC
-- MAGIC # Write the DataFrame to a Delta table, overwriting any existing data
-- MAGIC #diamonds.write.format("delta").mode("overwrite").saveAsTable("diamonds")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Drop the managed table
-- MAGIC #spark.sql("DROP TABLE IF EXISTS bronze_datafoundatstdev001.diamonds")

-- COMMAND ----------

Select count(*) from diamonds

-- COMMAND ----------

Describe Detail diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Read CSV using sparksql

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TEMPORARY VIEW diamonds_vw
USING CSV
OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create delta table from temp view

-- COMMAND ----------

DROP TABLE if exists diamonds_sql;

CREATE OR REPLACE TABLE diamonds_sql 
USING DELTA
--LOCATION '/mnt/deltalake/diamonds_sql'
AS
Select * from diamonds_vw


-- COMMAND ----------

DESCRIBE EXTENDED diamonds;

-- COMMAND ----------

--DROP TABLE diamonds;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create csv based table

-- COMMAND ----------

CREATE  TABLE diamonds_csv 
USING CSV
AS
Select * from diamonds_vw

-- COMMAND ----------

Select * from diamonds_csv 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC updates fail in csv table.
-- MAGIC Inserts work

-- COMMAND ----------

Update diamonds_csv SET color = 'Y' where color = 'E'

-- COMMAND ----------

Insert into diamonds_csv select * from diamonds_csv

-- COMMAND ----------

Drop table diamonds_csv