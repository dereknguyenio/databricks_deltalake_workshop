# Databricks notebook source
# MAGIC %md
# MAGIC # Bridging Fabric Lakehouses: Delta Change Data Feed for Seamless ETL - Hospital Vaccine ETL Example
# MAGIC ###### https://blog.fabric.microsoft.com/en-us/blog/bridging-fabric-lakehouses-delta-change-data-feed-for-seamless-etl?ft=All

# COMMAND ----------

username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
spark.sql(f"USE bronze_datafoundatstdev001.{username}")
base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
adls_path = f"{base_path}/{username}/people10m_optimized2"

#dbutils.widgets.text("adls_path", adls_path)

# COMMAND ----------

# Welcome to your new notebook
#https://blog.fabric.microsoft.com/en-us/blog/bridging-fabric-lakehouses-delta-change-data-feed-for-seamless-etl?ft=All
# Type here in the cell editor to add code!
Hospitals = [("Contoso_SouthEast", 10000, 20000), ("Contoso_NorthEast", 1000, 1500), ("Contoso_West", 7000, 10000), ("Contoso_North", 500, 700) ]
columns = ["Hospital","NumVaccinated","AvailableDoses", ]
spark.createDataFrame(data=Hospitals, schema = columns).write.format("delta").mode("overwrite").saveAsTable("Silver_HospitalVaccination")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Silver_HospitalVaccination

# COMMAND ----------

#Create/overwrite a table in a different lakehouse. This time we use the abfss file path instead of the shorthand version that will be used later to create/overwrite the delta table in a different lakehouse (our gold lakehouse)

import pyspark.sql.functions as F
spark.read.format("delta").table("Silver_HospitalVaccination").withColumn("VaccinationRate", F.col("NumVaccinated") / F.col("AvailableDoses")).withColumn("DeletedFlag", F.lit("N")) \
  .drop("NumVaccinated").drop("AvailableDoses") \
  .write.format("delta").mode("overwrite").saveAsTable("Gold_HospitalVaccination")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Gold_HospitalVaccination

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE Silver_HospitalVaccination SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC --CREATE TABLE myNewTable (id INT, name STRING, age INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update the silver lakehouse
# MAGIC UPDATE Silver_HospitalVaccination SET NumVaccinated = '11000' WHERE Hospital = 'Contoso_SouthEast'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete a record
# MAGIC DELETE from Silver_HospitalVaccination WHERE Hospital = 'Contoso_NorthEast'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a record
# MAGIC INSERT INTO Silver_HospitalVaccination VALUES ('Contoso_East', 500, 1500)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the changes in the SQL table
# MAGIC SELECT * FROM Silver_HospitalVaccination

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- view the changes using describe in SQL
# MAGIC describe history Silver_HospitalVaccination;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View a specific timestamp/commit in SQL
# MAGIC SELECT * FROM table_changes('Silver_HospitalVaccination', 1) order by _commit_timestamp DESC;

# COMMAND ----------

#Let's view the change data using PySpark

changes_df = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 1).table('Silver_HospitalVaccination')
display(changes_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect only the latest version for each Hospital by using a view to set up for our merge statement
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Silver_HospitalVaccination_latest_version as
# MAGIC SELECT * 
# MAGIC     FROM 
# MAGIC          (SELECT *, rank() over (partition by Hospital order by _commit_version desc) as rank
# MAGIC           FROM table_changes('Silver_HospitalVaccination', 1)
# MAGIC           WHERE _change_type !='update_preimage')--filters the 'before' values so we can only grab the updated values and not the old values
# MAGIC     WHERE rank=1 --if multiple changes occurred during a single commit then we get the most recent version of that

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Silver_HospitalVaccination_latest_version
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the changes to gold table, across lakehouses. Using the Silver Lakehouse as the default lakehouse
# MAGIC MERGE INTO Gold_HospitalVaccination t USING Silver_HospitalVaccination_latest_version s ON s.Hospital = t.Hospital --Joining our Gold Lakehouse to our view above on the Hospital
# MAGIC         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses -- When an update occurs, perform the neccessary calculations/transformation into our Gold table
# MAGIC         WHEN MATCHED AND s._change_type='delete' THEN UPDATE SET DeletedFlag = 'Y' -- If a hospital is deleted then we want to update the deleted flag to 'Y' and preserve the row
# MAGIC         WHEN NOT MATCHED THEN INSERT (Hospital, VaccinationRate, DeletedFlag) VALUES (s.Hospital, s.NumVaccinated/s.AvailableDoses, 'N')-- Insert new hospitals with our transformation logic or default values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Gold_HospitalVaccination 