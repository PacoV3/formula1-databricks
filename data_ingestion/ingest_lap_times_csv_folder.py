# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dludemy/raw/lap_times

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

final_laps_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/lap_times'))
