-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

-- Top 3 of each year
SELECT * FROM f1_presentation.driver_standings
WHERE rank IN (1,2,3)
ORDER BY race_year DESC, rank;

-- COMMAND ----------

-- Count of first rank and total wins for each driver
SELECT driver_name, COUNT(*) AS first_rank, SUM(wins) AS total_wins
FROM f1_presentation.driver_standings
WHERE rank = 1
GROUP BY driver_name
ORDER BY first_rank DESC;
