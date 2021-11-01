-- Databricks notebook source
DROP TABLE IF EXISTS f1_raw.constructors;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "/mnt/formula1dludemy/raw/constructors.json");

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "/mnt/formula1dludemy/raw/drivers.json");

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING json
OPTIONS(path "/mnt/formula1dludemy/raw/results.json");

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING json
OPTIONS(path "/mnt/formula1dludemy/raw/pit_stops.json", multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;
