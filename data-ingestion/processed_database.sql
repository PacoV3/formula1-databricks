-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dludemy/processed"

-- COMMAND ----------

SELECT * FROM f1_processed.circuits
