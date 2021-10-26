# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# SQL like way to filter
filtered_races_df = races_df.where('race_year = 2019 and round <= 5')

# COMMAND ----------

display(filtered_races_df)

# COMMAND ----------

# Python like way to filter
filtered_races_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))

# COMMAND ----------

display(filtered_races_df)
