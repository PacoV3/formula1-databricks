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

# This is a multiline JSON (the default for spark is an inline JSON)
# %fs head /mnt/formula1dludemy/raw/pit_stops.json

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

pits_schema = StructType(fields=[
      StructField('raceId', IntegerType(), False),
      StructField('driverId', IntegerType(), True),
      StructField('stop', IntegerType(), True),
      StructField('lap', IntegerType(), True),
      StructField('time', StringType(), True),
      StructField('duration', StringType(), True),
      StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pits_df = spark.read.schema(pits_schema).json(f'{raw_folder_path}/{v_file_date}/pit_stops.json', multiLine=True)

# COMMAND ----------

final_pits_df = pits_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

final_pits_df = add_ingestion_date(final_pits_df)

# COMMAND ----------

final_pits_df = add_data_source(final_pits_df, v_data_source)

# COMMAND ----------

final_pits_df = add_file_date(final_pits_df, v_file_date)

# COMMAND ----------

# final_pits_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')
# incremental_load(input_df = final_pits_df, partition_column = 'race_id', db ='f1_processed', table = 'pit_stops')
delta_lake_incremental_load(input_df = final_pits_df, partition_column = 'race_id',
                            keys = ['driver_id', 'stop'], db = 'f1_processed', table = 'pit_stops',
                            table_route = f'{processed_folder_path}/pit_stops')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY driver_id
# MAGIC ORDER BY driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
