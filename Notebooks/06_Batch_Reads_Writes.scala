// Databricks notebook source
// MAGIC %md
// MAGIC In this notebook, we will look at performing updates, deletes , upserts via merge statement on delta tables
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
// MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
// MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
// MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
// MAGIC adls_stg_path = f"{base_path}/{username}/people10m_stg"
// MAGIC adls_path = f"{base_path}/{username}/people10m_table"
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC display(dbutils.fs.ls('/databricks-datasets/learning-spark-v2/people/people-10m.delta/'))

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC query = f"""
// MAGIC CREATE TABLE IF NOT EXISTS {username}.people10m_stg
// MAGIC USING DELTA
// MAGIC LOCATION '{adls_stg_path}'
// MAGIC AS SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta/`
// MAGIC """
// MAGIC
// MAGIC spark.sql(query)
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC # Drop existing table
// MAGIC spark.sql("DROP TABLE IF EXISTS people10m")
// MAGIC
// MAGIC
// MAGIC # Create table (single statement)
// MAGIC spark.sql(f"""
// MAGIC CREATE TABLE people10m (
// MAGIC   id INT,
// MAGIC   firstName STRING,
// MAGIC   middleName STRING,
// MAGIC   lastName STRING,
// MAGIC   gender STRING,
// MAGIC   birthDate TIMESTAMP,
// MAGIC   ssn STRING,
// MAGIC   salary INT
// MAGIC )
// MAGIC USING DELTA
// MAGIC LOCATION '{adls_path}'
// MAGIC PARTITIONED BY (gender)
// MAGIC """)
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC Insert into people10m Select * from people10m_stg

// COMMAND ----------

// MAGIC %sql
// MAGIC Describe Detail people10m

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC Select count(*) from people10m where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %sql 
// MAGIC Delete From people10m  where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %md
// MAGIC Checking the version history of the table

// COMMAND ----------

// MAGIC %sql
// MAGIC Describe History people10m 

// COMMAND ----------

// MAGIC %md
// MAGIC Control versions maintained by delta table using logRetentionDuration property. Default is 30 days.

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE people10m SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 15 days')
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Querying the older version

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM people10m Version AS OF 1 where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %sql
// MAGIC Insert into people10m SELECT * FROM people10m Version AS OF 1 where firstName like 'A%' and salary between 40000 and 50000

// COMMAND ----------

// MAGIC %md
// MAGIC Lets perform some delete and update for the rows between 1 to 200

// COMMAND ----------

// MAGIC %sql
// MAGIC Delete From people10m where id between 1 and 100

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from people10m where id between 101 and 200
// MAGIC order by id asc

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from people10m where id between 1 and 100
// MAGIC order by id asc

// COMMAND ----------

// MAGIC %sql
// MAGIC Update people10m SET salary = salary * 1.20 where id between 101 and 200

// COMMAND ----------

// MAGIC %sql
// MAGIC Describe history  people10m

// COMMAND ----------

// MAGIC %md
// MAGIC Using merge and time travel to revert the changes done. Ensure to update the timestamp

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO people10m source
// MAGIC   USING people10m TIMESTAMP AS OF "2025-11-12T18:40:02" target
// MAGIC   ON source.id = target.id
// MAGIC   WHEN MATCHED THEN UPDATE SET *
// MAGIC   WHEN NOT MATCHED
// MAGIC   THEN INSERT * 

// COMMAND ----------

// MAGIC %sql
// MAGIC Select *  From people10m where id between 101 and 200

// COMMAND ----------

// MAGIC %sql
// MAGIC Delete  From people10m where id between 1 and 100

// COMMAND ----------

// MAGIC %md
// MAGIC Overwrite table ( instead of append ) using insert overwrite

// COMMAND ----------

// MAGIC %sql
// MAGIC Insert overwrite  people10m Select * from people10m TIMESTAMP AS OF "2025-11-12T18:40:02"

// COMMAND ----------

// MAGIC %sql
// MAGIC Select count(*) from people10m