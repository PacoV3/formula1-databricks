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

qualifying_df = spark.read.schema(qualifying_schema).json(f'{raw_folder_path}/{v_file_date}/qualifying', multiLine=True)

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id')

# COMMAND ----------

final_qualifying_df = add_ingestion_date(final_qualifying_df)

# COMMAND ----------

final_qualifying_df = add_data_source(final_qualifying_df, v_data_source)

# COMMAND ----------

final_qualifying_df = add_file_date(final_qualifying_df, v_file_date)

# COMMAND ----------

# final_qualifying_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')
incremental_load(input_df = final_qualifying_df, partition_column = 'race_id', db ='f1_processed', table = 'qualifying')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
