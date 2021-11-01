-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT COUNT(*) AS count_drivers
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT name, dob
FROM f1_processed.drivers
ORDER BY dob DESC
LIMIT 5;

-- COMMAND ----------

SELECT nationality, COUNT(*) AS count_drivers
FROM f1_processed.drivers
GROUP BY nationality
HAVING count_drivers > 100
ORDER BY count_drivers DESC;
