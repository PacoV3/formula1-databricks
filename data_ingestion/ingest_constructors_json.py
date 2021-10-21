# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using spark DataFrameReader

# COMMAND ----------

# Uses the Hive datatypes
constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json('/mnt/formula1dludemy/raw/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the DataFrame

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the output to a Parquet file

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet('/mnt/formula1dludemy/processed/constructors')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dludemy/processed/constructors
