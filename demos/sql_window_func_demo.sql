-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM f1_processed.drivers WHERE nationality = 'Mexican' ORDER BY nationality, age_rank;

-- COMMAND ----------

SELECT nationality, COUNT(*) AS nationality_count, DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) AS nationality_rank
FROM f1_processed.drivers
GROUP BY nationality
ORDER BY nationality_count DESC;

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM f1_processed.drivers
WHERE nationality = 'Mexican'
ORDER BY nationality, age_rank;
