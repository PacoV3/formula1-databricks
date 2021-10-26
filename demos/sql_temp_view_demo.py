# Databricks notebook source
# MAGIC %md
# MAGIC #### Access DataFrames using SQL
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Create temporary views on DataFrames
# MAGIC 2. Access the view from a SQL cell
# MAGIC 3. Access the view from a Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Create temporary views on DataFrames

# COMMAND ----------

race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Access the view from a SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Access the view from a Python cell

# COMMAND ----------

display(spark.sql('SELECT * FROM v_race_results WHERE race_year = 2020'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temprary Views
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Create global temprary views on DataFrames
# MAGIC 2. Access the view from a SQL cell
# MAGIC 3. Access the view from a Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql('SELECT * FROM global_temp.gv_race_results').show()
