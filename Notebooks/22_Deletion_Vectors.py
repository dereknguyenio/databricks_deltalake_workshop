# Databricks notebook source
username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
spark.sql(f"USE bronze_datafoundatstdev001.{username}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL people10m;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from people10m
# MAGIC where firstName like 'A%' and salary between 40000 and 50000

# COMMAND ----------

# MAGIC %sql 
# MAGIC Delete From people10m  where firstName like 'A%' and salary between 40000 and 50000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE people10m TO VERSION AS OF 12;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from people10m
# MAGIC where firstName like 'A%' and salary between 40000 and 50000