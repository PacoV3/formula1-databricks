# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# %fs ls /mnt/formula1dludemy/raw/lap_times

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

laps_df = spark.read.schema(laps_schema).csv(f'{raw_folder_path}/lap_times')

# COMMAND ----------

final_laps_df = laps_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

final_laps_df = add_ingestion_date(final_laps_df)

# COMMAND ----------

final_laps_df = add_data_source(final_laps_df, v_data_source)

# COMMAND ----------

final_laps_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

dbutils.notebook.exit('Success')
