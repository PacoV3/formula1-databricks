-- Databricks notebook source
DROP TABLE IF EXISTS f1_raw.lap_times;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS(path "/mnt/formula1dludemy/raw/lap_times");

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS(path "/mnt/formula1dludemy/raw/qualifying", multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;
