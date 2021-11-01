# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers') \
    .withColumnRenamed('name', 'driver_name')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors') \
    .withColumnRenamed('name', 'team')

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')  \
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

one_year_races = races_df.filter(races_df.race_year == 2020)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

joined_race_results = races_df \
        .join(results_df, results_df.race_id == races_df.race_id) \
        .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
        .join(circuits_df, one_year_races.circuit_id == circuits_df.circuit_id)

# COMMAND ----------

race_results = joined_race_results \
        .select(one_year_races.race_year, one_year_races.race_name, one_year_races.race_timestamp.alias('race_date'), circuits_df.location.alias('circuit_location'), drivers_df.nationality.alias('driver_nationality'), drivers_df.driver_name, drivers_df.number.alias('driver_number'), constructors_df.team, results_df.grid, results_df.fastest_lap_time.alias('fastest_lap'), results_df.time.alias('race_time'), results_df.points, results_df.position) \
        .withColumn('created_date', current_timestamp())

# COMMAND ----------

display(race_results.filter(race_results.race_year == 2009).orderBy(race_results.points.desc()))

# COMMAND ----------

race_results.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')
