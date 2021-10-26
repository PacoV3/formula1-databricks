# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year == 2020)

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(countDistinct(demo_df.race_name).alias('different_races')).show()

# COMMAND ----------

demo_df.filter(demo_df.driver_name == 'Sergio Pérez') \
    .select(countDistinct(demo_df.race_name).alias('different_races'), sum(demo_df.points).alias('sum_points')).show()

# COMMAND ----------

demo_df.groupBy(demo_df.driver_name) \
    .agg(sum('points').alias('sum_points'), countDistinct('race_name').alias('num_races')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Functions

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year.isin(2019,2020))

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupBy(demo_df.race_year, demo_df.driver_name) \
    .agg(sum('points').alias('sum_points'), countDistinct('race_name').alias('num_races'))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('sum_points'))

# COMMAND ----------

display(demo_grouped_df.withColumn('rank', rank().over(driverRankSpec)))
