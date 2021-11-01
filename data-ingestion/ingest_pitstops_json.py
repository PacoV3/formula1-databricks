# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

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

pits_df = spark.read.schema(pits_schema).json(f'{raw_folder_path}/pit_stops.json', multiLine=True)

# COMMAND ----------

final_pits_df = pits_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id')

# COMMAND ----------

final_pits_df = add_ingestion_date(final_pits_df)

# COMMAND ----------

final_pits_df = add_data_source(final_pits_df, v_data_source)

# COMMAND ----------

final_pits_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

dbutils.notebook.exit('Success')
