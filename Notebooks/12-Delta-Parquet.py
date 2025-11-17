# Databricks notebook source
# MAGIC %md
# MAGIC Create Parquet Table

# COMMAND ----------

username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
spark.sql(f"USE bronze_datafoundatstdev001.{username}")
base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
adls_path = f"{base_path}/{username}/diamonds_parquet"
dbutils.widgets.text("adls_path", adls_path)

# COMMAND ----------

spark.sql(f"""CREATE TABLE diamonds_parquet 
USING Parquet
LOCATION '{adls_path}'
AS
Select * from diamonds_sql""")

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe diamonds_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC update fails in parquet table

# COMMAND ----------

# MAGIC %sql 
# MAGIC Update diamonds_parquet SET color = 'F'

# COMMAND ----------

# MAGIC %md
# MAGIC Convert parquet to delta

# COMMAND ----------

spark.sql(f"""CONVERT TO DELTA parquet.`{adls_path}`""")

# COMMAND ----------

spark.sql(f"""Select count(*) from DELTA.`{adls_path}`""")

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail DELTA.`${adls_path}`

# COMMAND ----------

# MAGIC %sql 
# MAGIC Update  DELTA.`${adls_path}` SET color = 'F'

# COMMAND ----------

# MAGIC %md
# MAGIC Access using orignal parquet table name doesn't work even though we have converted the path as delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Update diamonds_parquet SET color = 'F'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from diamonds_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC for seamless access drop parquet table and recreate it as delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table diamonds_parquet

# COMMAND ----------

# MAGIC %md
# MAGIC IF you dont like managing the table as Delta.``, use create table to convert it

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table diamonds_parquet using delta location '${adls_path}'

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail diamonds_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC --Update doc_resrc SET ORID_DOC_NM='18'
# MAGIC --when
# MAGIC --DOC_RESRC_ID="12345"