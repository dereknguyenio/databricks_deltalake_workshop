// Databricks notebook source
// MAGIC %md
// MAGIC Create a container called autoloader<br>
// MAGIC Mount the container as explained in https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access <br>
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC #service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")
// MAGIC
// MAGIC #spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
// MAGIC #spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
// MAGIC #spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
// MAGIC #spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
// MAGIC #spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")
// MAGIC
// MAGIC #Replace
// MAGIC #		○ <scope> with the secret scope name from step 5.
// MAGIC #		○ <service-credential-key> with the name of the key containing the client secret.
// MAGIC #		○ <storage-account> with the name of the Azure storage account.
// MAGIC #		○ <application-id> with the Application (client) ID for the Microsoft Entra ID application.
// MAGIC #		○ <directory-id> with the Directory (tenant) ID for the Microsoft Entra ID application.
// MAGIC #You have now successfully connected your Azure Databricks workspace to your Azure Data Lake Storage Gen2 account.
// MAGIC
// MAGIC #service_credential = dbutils.secrets.get(scope="secretscope-deltalakeworkshop",key="secret")
// MAGIC
// MAGIC #spark.conf.set("fs.azure.account.auth.type.datalakedbsdkndev.dfs.core.windows.net", "OAuth")
// MAGIC #spark.conf.set("fs.azure.account.oauth.provider.type.datalakedbsdkndev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
// MAGIC #spark.conf.set("fs.azure.account.oauth2.client.id.datalakedbsdkndev.dfs.core.windows.net", "50aafe23-0a8e-45ff-8256-b05aa6c73fbf")
// MAGIC #spark.conf.set("fs.azure.account.oauth2.client.secret.datalakedbsdkndev.dfs.core.windows.net", service_credential)
// MAGIC #spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakedbsdkndev.dfs.core.windows.net", "https://login.microsoftonline.com/d536acfa-4d8d-4c41-8c8a-4cd88b251323/oauth2/token")
// MAGIC
// MAGIC
// MAGIC #configs = {"fs.azure.account.auth.type": "OAuth",
// MAGIC #           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC #           "fs.azure.account.oauth2.client.id": "50aafe23-0a8e-45ff-8256-b05aa6c73fbf", 
// MAGIC #           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="secretscope-deltalakeworkshop",key="deltalakeinaday-adb-sp-secret-value"),
// MAGIC #           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d536acfa-4d8d-4c41-8c8a-4cd88b251323/oauth2/token"}

// COMMAND ----------

// MAGIC %python
// MAGIC #dbutils.fs.mount(
// MAGIC #  source = "abfss://autoloader@datalakedbsdkndev.dfs.core.windows.net/",
// MAGIC #  mount_point = "/mnt/autoloader",
// MAGIC #  extra_configs = configs)
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Create a folder "population" in autoloader container <br>
// MAGIC Load the contents (csv files) of "population" dolder available in dataset folder in IP artifacts into population folder in autoloader container <br>

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("USE CATALOG bronze_datafoundatstdev001")
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
// MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
// MAGIC spark.sql(f"USE bronze_datafoundatstdev001.{username}")
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
// MAGIC
// MAGIC upload_path = f"{base_path}/{username}/autoloader/input"
// MAGIC checkpoint_path = f"{base_path}/{username}/autoloader/checkpoint"
// MAGIC write_path = f"{base_path}/{username}/autoloader/output"
// MAGIC
// MAGIC upload_path, checkpoint_path, write_path
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.mkdirs(upload_path)
// MAGIC dbutils.fs.mkdirs(checkpoint_path)
// MAGIC dbutils.fs.mkdirs(write_path)
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # Set up the stream to begin reading incoming files from the
// MAGIC # upload_path location.
// MAGIC df = (
// MAGIC     spark.readStream.format("cloudFiles")
// MAGIC     .option("cloudFiles.format", "csv")
// MAGIC     .option("header", "true")
// MAGIC     .schema("city string, year int, population long")
// MAGIC     .load(upload_path)
// MAGIC )
// MAGIC # Start the stream.
// MAGIC # Use the checkpoint_path location to keep a record of all files that
// MAGIC # have already been uploaded to the upload_path location.
// MAGIC # For those that have been uploaded since the last check,
// MAGIC # write the newly-uploaded files' data to the write_path location.
// MAGIC (
// MAGIC     df.writeStream.format("delta")
// MAGIC     .option("checkpointLocation", checkpoint_path)
// MAGIC     .option("mergeSchema", "true")
// MAGIC     .start(write_path)
// MAGIC )
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC # Create a Delta table pointing to the streamed data
// MAGIC # This command should be run after starting the streaming job
// MAGIC #By executing the SQL command below, you define a Delta table named population_delta that directly references the data at your write_path. This does not duplicate the data; it simply creates metadata that allows Spark to understand the structure and location of your data, making it queryable as a table.
// MAGIC
// MAGIC spark.sql(f"""
// MAGIC     CREATE TABLE IF NOT EXISTS population_delta
// MAGIC     USING DELTA
// MAGIC     LOCATION '{write_path}'
// MAGIC """)
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from population_delta;

// COMMAND ----------

// MAGIC %python
// MAGIC df_population = spark.read.format("delta").load(write_path)
// MAGIC
// MAGIC display(df_population)
// MAGIC
// MAGIC # Result:
// MAGIC # +----------------+------+------------+
// MAGIC # | city           | year | population |
// MAGIC # +================+======+============+
// MAGIC # | Seattle metro  | 2019 | 3406000    |
// MAGIC # +----------------+------+------------+
// MAGIC # | Seattle metro  | 2020 | 3433000    |
// MAGIC # +----------------+------+------------+
// MAGIC # | Portland metro | 2019 | 2127000    |
// MAGIC # +----------------+------+------------+
// MAGIC # | Portland metro | 2020 | 2151000    |
// MAGIC # +----------------+------+------------+

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # Create a delta table from the auto loader data and save it to db1 database
// MAGIC df_population.write.format("delta") \
// MAGIC     .mode("overwrite") \
// MAGIC     .saveAsTable("population_data")

// COMMAND ----------

dbutils.fs.rm(write_path, true)
dbutils.fs.rm(upload_path, true)

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC describe history population_data;

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history population_delta;