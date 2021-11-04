# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f'{processed_folder_path}/drivers') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f'{processed_folder_path}/constructors') \
    .withColumnRenamed('name', 'team')

# COMMAND ----------

results_df = spark.read.format('delta').load(f'{processed_folder_path}/results').filter(f"file_date == '{v_file_date}'") \
    .withColumnRenamed('time', 'race_time') \
    .withColumnRenamed('race_id', 'result_race_time')

# COMMAND ----------

races_df = spark.read.format('delta').load(f'{processed_folder_path}/races')  \
    .withColumnRenamed('name', 'race_name')  \
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f'{processed_folder_path}/circuits') \
    .withColumnRenamed('name', 'circuit_name') \
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
    .select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_location')

# COMMAND ----------

race_results = results_df.join(race_circuits_df, results_df.result_race_time == race_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results.select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number',
                               'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 'points', 'position', 'race_id') \
    .withColumn('created_date', current_timestamp())

# COMMAND ----------

final_df = add_file_date(final_df, v_file_date)

# COMMAND ----------

# race_results.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')
# incremental_load(input_df = final_df, partition_column = 'race_id', db ='f1_presentation', table = 'race_results')
delta_lake_incremental_load(input_df = final_df, partition_column = 'race_id',
                            keys = ['driver_name'], db = 'f1_presentation', table = 'race_results',
                            table_route = f'{presentation_folder_path}/race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
