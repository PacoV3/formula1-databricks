-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  circuitLocation STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dludemy/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dludemy/raw/races.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.races;
