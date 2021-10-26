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
