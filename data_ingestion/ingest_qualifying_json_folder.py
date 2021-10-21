# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json(f'{raw_folder_path}/qualifying', multiLine=True)

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id')

# COMMAND ----------

final_qualifying_df = add_ingestion_date(final_qualifying_df)

# COMMAND ----------

final_qualifying_df.write.mode('overwrite').parquet(f'{processed_folder_path}/qualifying')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/qualifying'))
