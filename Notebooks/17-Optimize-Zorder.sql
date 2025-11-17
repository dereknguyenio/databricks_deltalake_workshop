-- Databricks notebook source
-- MAGIC %md
-- MAGIC # High Performance Spark Queries with Databricks Delta
-- MAGIC Databricks Delta extends Apache Spark to simplify data reliability and boost Spark's performance.
-- MAGIC
-- MAGIC Building robust, high performance data pipelines can be difficult due to: _lack of indexing and statistics_, _data inconsistencies introduced by schema changes_ and _pipeline failures_, _and having to trade off between batch and stream processing_.
-- MAGIC
-- MAGIC With Databricks Delta, data engineers can build reliable and fast data pipelines. Databricks Delta provides many benefits including:
-- MAGIC * Faster query execution with indexing, statistics, and auto-caching support
-- MAGIC * Data reliability with rich schema validation and rransactional guarantees
-- MAGIC * Simplified data pipeline with flexible UPSERT support and unified Structured Streaming + batch processing on a single data source.
-- MAGIC
-- MAGIC ### Let's See How Databricks Delta Makes Spark Queries Faster!
-- MAGIC
-- MAGIC In this example, we will see how Databricks Delta can optimize query performance. We create a standard table using Parquet format and run a quick query to observe its latency. We then run a second query over the Databricks Delta version of the same table to see the performance difference between standard tables versus Databricks Delta tables. 
-- MAGIC
-- MAGIC Simply follow these 4 steps below:
-- MAGIC * __Step 1__ : Create a standard Parquet based table using data from US based flights schedule data
-- MAGIC * __Step 2__ : Run a query to to calculate number of flights per month, per originating airport over a year
-- MAGIC * __Step 3__ : Create the flights table using Databricks Delta and optimize the table.
-- MAGIC * __Step 4__ : Rerun the query in Step 2 and observe the latency. 
-- MAGIC
-- MAGIC __Note:__ _Throughout the example we will be building few tables with a 10s of million rows. Some of the operations may take a few minutes depending on your cluster configuration._

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
-- MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
-- MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
-- MAGIC adls_path = f"{base_path}/{username}/flights"
-- MAGIC dbutils.widgets.text("adls_path", adls_path)

-- COMMAND ----------

-- DBTITLE 1,Step 1: Write a Parquet based table using flights data

DROP TABLE IF EXISTS flights;

-- Create a standard table and import US based flights for year 2008
-- USING Clause: Specify parquet format for a standard table
-- PARTITIONED BY clause: Orginize data based on "Origin" column (Originating Airport code).
-- FROM Clause: Import data from a csv file. 
CREATE TABLE IF NOT EXISTS flights
USING parquet
LOCATION '${adls_path}'
PARTITIONED BY (Origin)
SELECT _c0 as Year, _c1 as Month, _c2 as DayofMonth, _c3 as DayOfWeek, _c4 as DepartureTime, _c5 as CRSDepartureTime, _c6 as ArrivalTime, 
  _c7 as CRSArrivalTime, _c8 as UniqueCarrier, _c9 as FlightNumber, _c10 as TailNumber, _c11 as ActualElapsedTime, _c12 as CRSElapsedTime, 
    _c13 as AirTime, _c14 as ArrivalDelay, _c15 as DepartureDelay, _c16 as Origin, _c17 as Destination, _c18 as Distance, 
    _c19 as TaxiIn, _c20 as TaxiOut, _c21 as Cancelled, _c22 as CancellationCode, _c23 as Diverted, _c24 as CarrierDelay, 
    _c25 as WeatherDelay, _c26 as NASDelay, _c27 as SecurityDelay, _c28 as LateAircraftDelay 
FROM csv.`dbfs:/databricks-datasets/asa/airlines/2008.csv`


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once step 1 completes, the standard "flights" table contains details of US flights for a year. 
-- MAGIC
-- MAGIC Next in Step 2, we run a query that get top 20 cities with highest monthly total flights on first day of week.

-- COMMAND ----------

-- DBTITLE 1,Step 2 : Query for top 20 cities with highest monthly total flights on first day of week
-- Get top 20 cities with highest monthly total flights on first day of week. & observe the latency! 
-- This query may take over a minute in certain cluster configurations. 
SELECT Month, Origin, count(*) as TotalFlights 
FROM flights
WHERE try_cast(DayOfWeek as int) = 1
GROUP BY Month, Origin 
ORDER BY TotalFlights DESC
LIMIT 20;

-- COMMAND ----------

select count(*)
FROM flights;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once step 2 completes, you can observe the latency with the standard "flights" table. 
-- MAGIC
-- MAGIC In step 3 and step 4, we do the same with a Databricks Delta table. This time, before running the query, we run the `OPTIMIZE` command with `ZORDER` to ensure data is optimized for faster retrieval. 

-- COMMAND ----------

-- DBTITLE 1,Step 3: Write a Databricks Delta based table using flights data
DROP TABLE IF EXISTS flights;

-- Create a standard table and import US based flights for year 2008
-- USING Clause: Specify "delta" format instead of the standard parquet format
-- PARTITIONED BY clause: Orginize data based on "Origin" column (Originating Airport code).
-- FROM Clause: Import data from a csv file.
CREATE TABLE flights
USING delta
LOCATION '${adls_path}'
PARTITIONED BY (Origin)
SELECT _c0 as Year, _c1 as Month, _c2 as DayofMonth, _c3 as DayOfWeek, _c4 as DepartureTime, _c5 as CRSDepartureTime, _c6 as ArrivalTime, 
  _c7 as CRSArrivalTime, _c8 as UniqueCarrier, _c9 as FlightNumber, _c10 as TailNumber, _c11 as ActualElapsedTime, _c12 as CRSElapsedTime, 
    _c13 as AirTime, _c14 as ArrivalDelay, _c15 as DepartureDelay, _c16 as Origin, _c17 as Destination, _c18 as Distance, 
    _c19 as TaxiIn, _c20 as TaxiOut, _c21 as Cancelled, _c22 as CancellationCode, _c23 as Diverted, _c24 as CarrierDelay, 
    _c25 as WeatherDelay, _c26 as NASDelay, _c27 as SecurityDelay, _c28 as LateAircraftDelay 
FROM csv.`dbfs:/databricks-datasets/asa/airlines/2008.csv`;

-- COMMAND ----------

-- DBTITLE 1,Step 3 Continued: OPTIMIZE the Databricks Delta table
-- OPTIMIZE consolidates files and orders the Databricks Delta table data by DayofWeek under each partition for faster retrieval
OPTIMIZE flights ZORDER BY (DayofWeek);

-- COMMAND ----------

-- DBTITLE 1,Step 4 : Rerun the query from Step 2 and observe the latency
-- Get top 20 cities with highest monthly total flights on first day of week. & observe the latency! 
-- This query may take over a minute in certain cluster configurations. 
SELECT Month, Origin, count(*) as TotalFlights 
FROM flights
WHERE try_cast(DayOfWeek as int) = 1 
GROUP BY Month, Origin 
ORDER BY TotalFlights DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The query over the Databricks Delta table runs much faster after `OPTIMIZE` is run. How much faster the query runs can depend on the configuration of the cluster you are running on, however should be **5-10X** faster compared to the standard table. 