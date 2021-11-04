# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# %fs head /mnt/formula1dludemy/raw/results.json

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('laps', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', StringType(), True),
    StructField('statusId', IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed('resultId', 'result_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('positionText', 'position_text') \
    .withColumnRenamed('positionOrder', 'position_order') \
    .withColumnRenamed('fastestLap', 'fastest_lap') \
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

results_renamed_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

results_renamed_df = add_data_source(results_renamed_df, v_data_source)

# COMMAND ----------

results_renamed_df = add_file_date(results_renamed_df, v_file_date)

# COMMAND ----------

# display(results_renamed_df)

# COMMAND ----------

final_results_df = results_renamed_df.drop(results_renamed_df.statusId)

# COMMAND ----------

final_dedeuped_df = final_results_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# for id_list in final_results_df.select('race_id').distinct().collect():
#     if spark._jsparkSession.catalog().tableExists('f1_processed.results'):
#         spark.sql(f'ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {id_list.race_id})')

# COMMAND ----------

# final_results_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

# spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

# final_results_df = final_results_df.select('result_id', 'driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text', 'position_order', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time', 'fastest_lap_speed', 'ingestion_date', 'data_source', 'file_date', 'race_id')

# COMMAND ----------

# if spark._jsparkSession.catalog().tableExists('f1_processed.results'):
#     final_results_df.write.mode('overwrite').insertInto('f1_processed.results')
# else:
#     final_results_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# Function that implements the 2nd method
# incremental_load(input_df = final_results_df, partition_column = 'race_id', db ='f1_processed', table = 'results')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 3 - With Delta Lake

# COMMAND ----------

# from delta.tables import DeltaTable

# COMMAND ----------

# if spark._jsparkSession.catalog().tableExists('f1_processed.results'):
#     delta_table = DeltaTable.forPath(spark, '/mnt/formula1dludemy/processed/results')
#     delta_table.alias('tgt').merge(
#         final_results_df.alias('src'),
#         'tgt.result_id = src.result_id AND tgt.race_id = src.race_id') \
#         .whenMatchedUpdateAll() \
#         .whenNotMatchedInsertAll() \
#         .execute()
# else:
#     final_results_df.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable('f1_processed.results')

# COMMAND ----------

delta_lake_incremental_load(input_df = final_dedeuped_df, partition_column = 'race_id',
                            keys = ['result_id'], db = 'f1_processed', table = 'results',
                            table_route = f'{processed_folder_path}/results')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, file_date, COUNT(1) AS races
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id, file_date
# MAGIC HAVING races > 1
# MAGIC ORDER BY race_id DESC, driver_id DESC;
