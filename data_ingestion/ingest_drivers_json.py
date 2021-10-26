# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 0 - Get paths and functions from another notebook

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark DataFrameReader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

# display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion_date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_complete_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('driverRef', 'driver_ref') \
    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

# COMMAND ----------

drivers_complete_df = add_ingestion_date(drivers_complete_df)

# COMMAND ----------

drivers_complete_df = add_data_source(drivers_complete_df, v_data_source)

# COMMAND ----------

# display(drivers_complete_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_complete_df.drop(drivers_complete_df.url)

# COMMAND ----------

# display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the output to processed container in Parquet format

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')
