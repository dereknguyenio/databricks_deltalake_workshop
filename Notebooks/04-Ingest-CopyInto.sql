-- Databricks notebook source
DROP TABLE IF EXISTS default.loan_risks_upload;

CREATE TABLE default.loan_risks_upload (
  loan_id BIGINT,
  funded_amnt INT,
  paid_amnt DOUBLE,
  addr_state STRING
) USING DELTA;

COPY INTO default.loan_risks_upload
FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
FILEFORMAT = PARQUET;

SELECT * FROM default.loan_risks_upload;