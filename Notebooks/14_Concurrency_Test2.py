# Databricks notebook source
username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
spark.sql(f"USE bronze_datafoundatstdev001.{username}")
base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
adls_path = f"{base_path}/{username}/concurrency_test"
dbutils.widgets.text("adls_path", adls_path)

# COMMAND ----------

# COMMAND ----------
import time, random

def write_update(id_val, user):
    for i in range(5):
        try:
            spark.sql(f"""
                UPDATE concurrency_test
                SET 
                    value = '{user}_{i}',
                    updated_by = '{user}',
                    updated_at = current_timestamp()
                WHERE id = {id_val}
            """)
            print(f"[SUCCESS] {user} updated id={id_val} iteration={i}")
        except Exception as e:
            print(f"[FAIL] {user} FAILED iteration={i}: {str(e)}")
        time.sleep(random.uniform(0.1,0.4))


# COMMAND ----------

# COMMAND ----------
# CHANGE THIS NAME FOR EACH USER
write_update(1, "UserA")
