# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

laps_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

laps_df = spark.read.schema(laps_schema).csv(f'{raw_folder_path}/{v_file_date}/lap_times')

# COMMAND ----------

final_laps_df = laps_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

final_laps_df = add_ingestion_date(final_laps_df)

# COMMAND ----------

final_laps_df = add_data_source(final_laps_df, v_data_source)

# COMMAND ----------

final_laps_df = add_file_date(final_laps_df, v_file_date)

# COMMAND ----------

# final_laps_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')
# incremental_load(input_df = final_laps_df, partition_column = 'race_id', db ='f1_processed', table = 'lap_times')}
delta_lake_incremental_load(input_df = final_laps_df, partition_column = 'race_id',
                            keys = ['driver_id', 'lap'], db = 'f1_processed', table = 'lap_times',
                            table_route = f'{processed_folder_path}/lap_times')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
