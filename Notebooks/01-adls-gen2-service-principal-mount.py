# Databricks notebook source
# MAGIC %md
# MAGIC Create a data lake storage account<br>
# MAGIC Create a container deltalake inside the storage account<br>
# MAGIC Follow the steps in the below link to grant access to mount the storage account to databricks workspace<br>

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access

# COMMAND ----------

# MAGIC %md
# MAGIC Steps to mount the container provided below <br>
# MAGIC Update the key vault and scope name

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Configure authentication for mounting
#service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

#spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
#spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

#Replace
#		○ <scope> with the secret scope name from step 5.
#		○ <service-credential-key> with the name of the key containing the client secret.
#		○ <storage-account> with the name of the Azure storage account.
#		○ <application-id> with the Application (client) ID for the Microsoft Entra ID application.
#		○ <directory-id> with the Directory (tenant) ID for the Microsoft Entra ID application.
#You have now successfully connected your Azure Databricks workspace to your Azure Data Lake Storage Gen2 account.

#service_credential = dbutils.secrets.get(scope="secretscope-deltalakeworkshop",key="secret")

#spark.conf.set("fs.azure.account.auth.type.datalakedbsdkndev.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.datalakedbsdkndev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.datalakedbsdkndev.dfs.core.windows.net", "50aafe23-0a8e-45ff-8256-b05aa6c73fbf")
#spark.conf.set("fs.azure.account.oauth2.client.secret.datalakedbsdkndev.dfs.core.windows.net", service_credential)
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakedbsdkndev.dfs.core.windows.net", "https://login.microsoftonline.com/d536acfa-4d8d-4c41-8c8a-4cd88b251323/oauth2/token")


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="secretscope-kv-df-dev001",key="datafoundation-clientid"), 
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="secretscope-kv-df-dev001",key="datafoundation-clientsecret3"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/2b3ba1cb-159d-464a-b93b-0cccdfeaf0bf/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC Update the storage account name as per your environment

# COMMAND ----------

# DBTITLE 1,Mount filesystem
dbutils.fs.mount(
  source = "abfss://deltalake@datalakedbsdkndev.dfs.core.windows.net/",
  mount_point = "/mnt/deltalake",
  extra_configs = configs if 'configs' in globals() else None)



# COMMAND ----------

# MAGIC %sql
# MAGIC --#dbutils.fs.unmount("/mnt/deltalake")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create a sample database, a sample table, load some data 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Database if not exists bronze_datafoundatstdev001.db1 ;--LOCATION 'abfss://bronze@datafoundatstdev001.dfs.core.windows.net/';
# MAGIC
# MAGIC DROP TABLE IF EXISTS bronze_datafoundatstdev001.db1.loan_risks_upload;
# MAGIC
# MAGIC CREATE TABLE bronze_datafoundatstdev001.db1.loan_risks_upload (
# MAGIC   loan_id BIGINT,
# MAGIC   funded_amnt INT,
# MAGIC   paid_amnt DOUBLE,
# MAGIC   addr_state STRING
# MAGIC ) USING DELTA LOCATION 'abfss://bronze@datafoundatstdev001.dfs.core.windows.net/loan_risks_upload';
# MAGIC
# MAGIC COPY INTO bronze_datafoundatstdev001.db1.loan_risks_upload
# MAGIC FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
# MAGIC FILEFORMAT = PARQUET;
# MAGIC
# MAGIC SELECT count(*) FROM bronze_datafoundatstdev001.db1.loan_risks_upload;
# MAGIC
# MAGIC -- Result:
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | loan_id | funded_amnt | paid_amnt | addr_state |
# MAGIC -- +=========+=============+===========+============+
# MAGIC -- | 0       | 1000        | 182.22    | CA         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | 1       | 1000        | 361.19    | WA         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- | 2       | 1000        | 176.26    | TX         |
# MAGIC -- +---------+-------------+-----------+------------+
# MAGIC -- ...

# COMMAND ----------

# MAGIC %md
# MAGIC Navigate to the container and folder in datalake gen 2 account and show the directory structure, data file and log file format

# COMMAND ----------

# DBTITLE 1,Basic Insert
# MAGIC %sql
# MAGIC Insert into bronze_datafoundatstdev001.db1.loan_risks_upload Select * from bronze_datafoundatstdev001.db1.loan_risks_upload

# COMMAND ----------

# DBTITLE 1,Read Data
# MAGIC %sql
# MAGIC Select count(*) from bronze_datafoundatstdev001.db1.loan_risks_upload

# COMMAND ----------

# DBTITLE 1,Select with Join
# MAGIC %sql
# MAGIC select x.addr_state,y.addr_state from bronze_datafoundatstdev001.db1.loan_risks_upload x inner join bronze_datafoundatstdev001.db1.loan_risks_upload Y on x.loan_id = y.loan_id where x.paid_amnt between 400 and 500

# COMMAND ----------

# Drop the table
#spark.sql("DROP TABLE IF EXISTS loan_risks_upload")
# Clear the delta log
#dbutils.fs.rm("/mnt/deltalake/loan_risks_upload/_delta_log", True)