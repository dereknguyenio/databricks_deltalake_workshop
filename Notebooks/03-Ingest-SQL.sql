-- Databricks notebook source
-- MAGIC %scala
-- MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Read SQL Database. Provide your username and password and sql database

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val jdbcUsername = "*******"
-- MAGIC val jdbcPassword = "*******"
-- MAGIC val jdbcHostname = "sqldb.database.windows.net"
-- MAGIC // Create a user SQL DB and key in the credentials. Alternatively, create keyvault, store secret password and use dbutils.secrets ( as done in 01-adls-gen2-service-principal-mount.dbc) to retrive passwods in a secure way
-- MAGIC val jdbcPort = 1433
-- MAGIC val jdbcDatabase = "sample"
-- MAGIC
-- MAGIC // Create the JDBC URL without passing in the user and password parameters.
-- MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
-- MAGIC
-- MAGIC // Create a Properties() object to hold the parameters.
-- MAGIC import java.util.Properties
-- MAGIC val connectionProperties = new Properties()
-- MAGIC
-- MAGIC connectionProperties.put("user", s"${jdbcUsername}")
-- MAGIC connectionProperties.put("password", s"${jdbcPassword}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Initialize the driver and load data to dataframe. Rewrite the query based on your table design

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
-- MAGIC connectionProperties.setProperty("Driver", driverClass)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Note: The parentheses are required.
-- MAGIC val pushdown_query = "(SELECT  Id,[Data1],[Data2],[Data3],[at_1],[at_2],[at_3],[Data2_dt] FROM dbo.LargeTbl3 where id < 10) Largetbl"
-- MAGIC val df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create temp view from dataframe

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.createOrReplaceTempView("Largetbl_vw")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create delta table from view

-- COMMAND ----------

DROP TABLE if exists db1.Largetbl;
CREATE OR REPLACE TABLE db1.Largetbl
USING DELTA
LOCATION '/mnt/deltalake/largetbl'
AS
Select * from Largetbl_vw

-- COMMAND ----------

Describe detail db1.Largetbl