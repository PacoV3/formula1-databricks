# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

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
    db_table = f'{db}.{table}'
    if spark._jsparkSession.catalog().tableExists(db_table):
        selected_df.write.mode('overwrite').insertInto(db_table)
    else:
        selected_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(db_table)

# COMMAND ----------

def delta_lake_incremental_load(input_df, partition_column, keys, db, table, table_route):
    # When using a merge, is a good practice to add the partition column on the condition to make the process faster
    spark.conf.set('spark.databricks.optimizer.dynamicPartitionPruning', 'true')
    # Set the db and table
    db_table = f'{db}.{table}'
    # Create the merge condition
    merge_condition = ''
    for key in keys:
        merge_condition += f'tgt.{key} = src.{key} AND '
    merge_condition += f'tgt.{partition_column} = src.{partition_column}'
    # Merge or create the table
    if spark._jsparkSession.catalog().tableExists(db_table):
        delta_table = DeltaTable.forPath(spark, table_route)
        delta_table.alias('tgt').merge(input_df.alias('src'), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_column).format('delta').saveAsTable(db_table)
