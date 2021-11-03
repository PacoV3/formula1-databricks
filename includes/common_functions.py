# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def add_data_source(input_df, data_source):
    output_df = input_df.withColumn('data_source', lit(data_source))
    return output_df

# COMMAND ----------

def add_file_date(input_df, file_date):
    output_df = input_df.withColumn('file_date', lit(file_date))
    return output_df

# COMMAND ----------

def prepare_df_for_load(input_df, partition_column):
    corrected_columns = input_df.schema.names
    corrected_columns.remove(partition_column)
    corrected_columns.append(partition_column)
    return input_df.select(corrected_columns)

# COMMAND ----------

def incremental_load(input_df, partition_column, db, table):
    # Change the overwrite mode to be dynamic
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    # DataFrame with the partitionBy column at the end
    selected_df = prepare_df_for_load(input_df, partition_column)
    # Insert the data
    location = f'{db}.{table}'
    if spark._jsparkSession.catalog().tableExists(location):
        selected_df.write.mode('overwrite').insertInto(location)
    else:
        selected_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(location)
