# Databricks notebook source
username = spark.sql("SELECT current_user()").first()[0].split('@')[0]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze_datafoundatstdev001.`{username}`")
spark.sql(f"USE bronze_datafoundatstdev001.{username}")
base_path = "abfss://bronze@datafoundatstdev001.dfs.core.windows.net/workshop"
adls_path = f"{base_path}/{username}/concurrency_test"
dbutils.widgets.text("adls_path", adls_path)

# COMMAND ----------

# COMMAND ----------

# Clean existing table
spark.sql("DROP TABLE IF EXISTS concurrency_test")

# Create fresh delta table
spark.sql("""
CREATE TABLE concurrency_test (
  id INT,
  value STRING,
  updated_by STRING,
  updated_at TIMESTAMP
) USING DELTA
""")

# Seed rows
spark.sql("""
INSERT INTO concurrency_test VALUES
(1,'A','init',current_timestamp()),
(2,'B','init',current_timestamp()),
(3,'C','init',current_timestamp())
""")

display(spark.table("concurrency_test"))


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
write_update(1, "Instructor")


# COMMAND ----------

# COMMAND ----------
display(spark.table("concurrency_test"))


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY concurrency_test
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # COMMAND ----------
# MAGIC ### HOW TO RUN THE CONCURRENCY TEST
# MAGIC 1. Open a **second notebook** (User notebook).
# MAGIC 2. In the second notebook, call:
# MAGIC write_update(1, "UserA")
# MAGIC
# MAGIC 3. At the same time, in this instructor notebook, call: write_update(1, "Instructor")
# MAGIC
# MAGIC 4. You will see Delta conflict messages.
# MAGIC
# MAGIC
# MAGIC