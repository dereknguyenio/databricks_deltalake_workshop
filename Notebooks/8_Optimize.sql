-- Databricks notebook source
-- MAGIC %md 
-- MAGIC exploring optimize to compact delta table. 

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

CREATE TABLE people10m_optimized2
LOCATION '${adls_path}'
AS 
Select * from people10m

-- COMMAND ----------

select * from people10m_optimized2

-- COMMAND ----------

Describe detail people10m_optimized2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Identifying the number of files in the table

-- COMMAND ----------

select distinct _metadata.file_path from people10m_optimized2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Perform some DML to create more parquet files

-- COMMAND ----------

Delete from people10m_optimized2 where salary between 20000 and 50000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check parquet files

-- COMMAND ----------

select distinct _metadata.file_path from people10m_optimized2

-- COMMAND ----------

Optimize people10m_optimized2

-- COMMAND ----------

select distinct _metadata.file_path from people10m_optimized2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC database_names_filter = "admin"
-- MAGIC try:
-- MAGIC     dbs = spark.sql(f"SHOW DATABASES LIKE '{database_names_filter}'").select("databaseName").collect()
-- MAGIC     dbs = [(row.databaseName) for row in dbs]
-- MAGIC     for database_name in dbs:
-- MAGIC         print(f"Found database: {database_name}, performing actions on all its tables..")
-- MAGIC         tables = spark.sql(f"SHOW TABLES FROM `{database_name}`").select("tableName").collect()
-- MAGIC         tables = [(row.tableName) for row in tables]
-- MAGIC         for table_name in tables:
-- MAGIC        #spark.sql(f"ALTER TABLE `{database_name}`.`{table_name}` SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 7 days')")
-- MAGIC             spark.sql(f"OPTIMIZE `{database_name}`.`{table_name}`")   
-- MAGIC 	    #spark.sql(f"VACUUM `{database_name}`.`{table_name}`")
-- MAGIC             #spark.sql(f"ANALYZE TABLE `{database_name}`.`{table_name}` COMPUTE STATISTICS")
-- MAGIC except Exception as e:
-- MAGIC     raise Exception(f"Exception:`{str(e)}`")

-- COMMAND ----------

optimize people10m where gender = 'F' 
--works only for partitioned columns