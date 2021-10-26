# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv fle

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 0 - Get paths and functions from another notebook

# COMMAND ----------

# dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

circuits_df = spark.read.csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

# circuits_df.show(2)

# COMMAND ----------

# Import all the types in the data that we will have to use
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# StructType represents rows inside a DataFrame and StructType represents the columns
circuits_schema = StructType(fields=[
    StructField('circuitId', IntegerType(), False),
    StructField('circuitRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('location', StringType(), True),
    StructField('country', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('lng', DoubleType(), True),
    StructField('alt', IntegerType(), True),
    StructField('url', StringType(), True),
])

# COMMAND ----------

# The correct way to import data from a CSV (including the headers)
circuits_df = spark.read.option('header',True).schema(circuits_schema).csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

# circuits_df.printSchema()

# COMMAND ----------

# circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select only the required columns
# MAGIC 
# MAGIC How to select a specific column
# MAGIC * "column_name"
# MAGIC 
# MAGIC This ones achieve the same result but we can apply column base functions
# MAGIC * df_name.column_name
# MAGIC * df_name["column_name"]
# MAGIC * from pyspark.sql.functions import col -> col("column_name").alias("new_name")

# COMMAND ----------

# Import the col function to select the columns
from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude')

# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion date to the DataFrame

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp())
# To create a column from one value
# from pyspark.sql.functions import lit -> .withColumn("ingestion_date", lit("X"))
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df = add_data_source(circuits_final_df, v_data_source)

# COMMAND ----------

# display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

dbutils.notebook.exit('Success')
