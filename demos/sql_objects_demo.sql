-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson Objectives
-- MAGIC 
-- MAGIC 1. Check the Spark SQL documentation
-- MAGIC 2. Create a Database demo
-- MAGIC 3. Put a data tab in the UI
-- MAGIC 4. Use the SHOW command
-- MAGIC 5. Use the DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC 
-- MAGIC 1. Create managed tables using Python
-- MAGIC 2. Create managed tables using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table
-- MAGIC 
-- MAGIC ##### A DROP on Managed Tables drops the data as well and on External Tables just the table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode('overwrite').format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC 
-- MAGIC 1. Create external tables using Python
-- MAGIC 2. Create external tables using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').mode('overwrite') \
-- MAGIC     .option('path', f'{presentation_folder_path}/race_results_ext_table_py').saveAsTable('demo.race_results_ext_table_py')

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_table_py;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_ext_table_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_nationality STRING,
  driver_name STRING,
  driver_number INT,
  team STRING,
  grid INT,
  fastest_lap STRING,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1dludemy/presentation/race_results_ext_table_sql";

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_table_sql;

-- COMMAND ----------

-- INSERT INTO TABLE demo.race_results_ext_table_sql
-- SELECT * FROM demo.race_results_ext_table_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_table_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC 
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Gloabal Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results;
