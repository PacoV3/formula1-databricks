# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 0 - Import variables and functions from another notebook

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using spark DataFrameReader

# COMMAND ----------

# Uses the Hive datatypes
constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f'{raw_folder_path}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the DataFrame

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref')

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

constructors_final_df = add_data_source(constructors_final_df, v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the output to a Parquet file

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')
