# Databricks notebook source
# MAGIC %md
# MAGIC # Notebooks Introduction
# MAGIC * UI Intro
# MAGIC * Magic Commands

# COMMAND ----------

msg = "Hello"

# COMMAND ----------

print(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello"

# COMMAND ----------

# MAGIC %scala
# MAGIC val msg = "Hello"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/COVID/USAFacts/

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/databricks-datasets/COVID/USAFacts/covid_confirmed_usafacts.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ps
