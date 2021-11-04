# Databricks notebook source
# MAGIC %md
# MAGIC ### Assignment for the Data Ingestion of the races.csv

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Look and preview the data

# COMMAND ----------

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

# %fs ls /mnt/formula1dludemy/raw

# COMMAND ----------

# %fs head dbfs:/mnt/formula1dludemy/raw/races.csv

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Read the data

# COMMAND ----------

# Import datatypes to create an Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('year', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('circuitId', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('date', DateType(), True),
    StructField('time', StringType(), True)
])

# COMMAND ----------

# The correct way to import data from a CSV (including the headers)
races_df = spark.read.option('header',True).schema(races_schema).csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

# display(races_df)

# COMMAND ----------

# races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns to the right name

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Select the required data and add columns

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, to_timestamp

# COMMAND ----------

races_final_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_final_df = races_final_df.select(col('race_id'), col('race_year'), col('round'), col('circuit_id'), col('name'), col('race_timestamp'))

# COMMAND ----------

races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

races_final_df = add_data_source(races_final_df, v_data_source)

# COMMAND ----------

races_final_df = add_file_date(races_final_df, v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Save the data as a Parquet into the container

# COMMAND ----------

# races_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

# display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Partition the data by year, to improve performance in a multi node cluster

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Success')
