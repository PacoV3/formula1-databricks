# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races') \
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

races_df = races_df.filter(races_df.race_year == 2019)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner Joins

# COMMAND ----------

# Inner Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner') \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outer Joins

# COMMAND ----------

filtered_circuits_df = circuits_df.filter(circuits_df.circuit_id < 70)

# COMMAND ----------

display(filtered_circuits_df)

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

# Left Outer Join
race_circuits_df = filtered_circuits_df.join(races_df, filtered_circuits_df.circuit_id == races_df.circuit_id, 'left') \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select([count(when(col(c).isNull(), c)).alias(c) for c in race_circuits_df.columns]).show()

# COMMAND ----------

# Right Outer Join
race_circuits_df = filtered_circuits_df.join(races_df, filtered_circuits_df.circuit_id == races_df.circuit_id, 'right') \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select([count(when(col(c).isNull(), c)).alias(c) for c in race_circuits_df.columns]).show()

# COMMAND ----------

# Full Outer Join
race_circuits_df = filtered_circuits_df.join(races_df, filtered_circuits_df.circuit_id == races_df.circuit_id, 'full') \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select([count(when(col(c).isNull(), c)).alias(c) for c in race_circuits_df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semi Joins

# COMMAND ----------

# Semi Join
race_circuits_df = filtered_circuits_df.join(races_df, filtered_circuits_df.circuit_id == races_df.circuit_id, 'semi')

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti Joins

# COMMAND ----------

# Anti Join
race_circuits_df = filtered_circuits_df.join(races_df, filtered_circuits_df.circuit_id == races_df.circuit_id, 'anti')

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross Joins

# COMMAND ----------

# Semi Outer Join
race_circuits_df = races_df.crossJoin(filtered_circuits_df)

# COMMAND ----------

display(race_circuits_df)
