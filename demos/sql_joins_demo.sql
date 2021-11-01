-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE f1_presentation;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS SELECT race_year, driver_name, team, sum_points, wins, rank
FROM f1_presentation.driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS SELECT race_year, driver_name, team, sum_points, wins, rank
FROM f1_presentation.driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020;

-- COMMAND ----------

-- Drivers that raced on both years (2018 and 2020) = 15 drivers
SELECT d_2018.race_year, d_2018.driver_name, d_2018.team, d_2018.rank, d_2020.race_year, d_2020.driver_name, d_2020.team, d_2020.rank
FROM v_driver_standings_2018 AS d_2018
JOIN v_driver_standings_2020 AS d_2020
ON d_2020.driver_name = d_2018.driver_name
ORDER BY d_2020.rank;

-- COMMAND ----------

-- All the drivers from the left side (2018) = 20 drivers
SELECT d_2018.race_year, d_2018.driver_name, d_2018.team, d_2018.rank, d_2020.race_year, d_2020.driver_name, d_2020.team, d_2020.rank
FROM v_driver_standings_2018 AS d_2018
LEFT JOIN v_driver_standings_2020 AS d_2020
ON d_2020.driver_name = d_2018.driver_name
ORDER BY d_2020.rank;

-- COMMAND ----------

-- All the drivers from the right side (2020) = 24 drivers
SELECT d_2018.race_year, d_2018.driver_name, d_2018.team, d_2018.rank, d_2020.race_year, d_2020.driver_name, d_2020.team, d_2020.rank
FROM v_driver_standings_2018 AS d_2018
RIGHT JOIN v_driver_standings_2020 AS d_2020
ON d_2020.driver_name = d_2018.driver_name
ORDER BY d_2020.rank;

-- COMMAND ----------

-- All drivers from both years (2018 and 2020) = 29 drivers
SELECT d_2018.race_year, d_2018.driver_name, d_2018.team, d_2018.rank, d_2020.race_year, d_2020.driver_name, d_2020.team, d_2020.rank
FROM v_driver_standings_2018 AS d_2018
FULL JOIN v_driver_standings_2020 AS d_2020
ON d_2020.driver_name = d_2018.driver_name
ORDER BY d_2020.rank;

-- COMMAND ----------

-- Only drivers from the right side that are present in both sides = 15 drivers
SELECT d_2018.race_year, d_2018.driver_name, d_2018.team, d_2018.rank
FROM v_driver_standings_2018 AS d_2018
SEMI JOIN v_driver_standings_2020 AS d_2020
ON d_2020.driver_name = d_2018.driver_name
ORDER BY d_2018.rank;

-- COMMAND ----------

-- Drivers that race on 2018 but not in 2020 (the missing ones on the query on top) = 5
SELECT d_2018.race_year, d_2018.driver_name, d_2018.team, d_2018.rank
FROM v_driver_standings_2018 AS d_2018
ANTI JOIN v_driver_standings_2020 AS d_2020
ON d_2020.driver_name = d_2018.driver_name
ORDER BY d_2018.rank;

-- COMMAND ----------

-- All the records from 2018 vs all the records from 2020
SELECT d_2018.race_year, d_2018.driver_name, d_2018.team, d_2018.rank, d_2020.race_year, d_2020.driver_name, d_2020.team, d_2020.rank
FROM v_driver_standings_2018 AS d_2018
CROSS JOIN v_driver_standings_2020 AS d_2020
