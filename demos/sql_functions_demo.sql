-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] AS forename, SPLIT(name, ' ')[1] AS surname
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, current_timestamp AS current_timestamp
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy') AS new_dob
FROM f1_processed.drivers;
