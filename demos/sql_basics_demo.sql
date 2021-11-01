-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED f1_processed.drivers;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers LIMIT 3;

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM f1_processed.drivers
WHERE nationality = "Mexican"
ORDER BY date_of_birth DESC;
