# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# This is a multiline JSON (the default for spark is an inline JSON)
%fs
head /mnt/formula1dludemy/raw/pit_stops.json

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

final_pits_df.write.mode('overwrite').parquet(f'{processed_folder_path}/pit_stops')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/pit_stops'))
