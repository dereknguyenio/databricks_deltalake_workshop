# Databricks notebook source
# MAGIC %sql
# MAGIC -- Catalog-level
# MAGIC GRANT USE CATALOG ON CATALOG bronze_datafoundatstdev001 TO `data_engineers`;
# MAGIC GRANT CREATE ON CATALOG bronze_datafoundatstdev001 TO `data_engineers`;
# MAGIC
# MAGIC -- Schema-level
# MAGIC GRANT USAGE, CREATE, MODIFY
# MAGIC ON SCHEMA bronze_datafoundatstdev001.db1 
# MAGIC TO `data_engineers`;
# MAGIC
# MAGIC
# MAGIC