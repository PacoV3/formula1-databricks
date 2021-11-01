# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy('race_year', 'driver_name', 'driver_nationality', 'team') \
    .agg(count(when(race_results_df.position == 1, True)).alias('wins'),
        sum('points').alias('sum_points'),)

# COMMAND ----------

display(driver_standings_df.filter('race_year == 2020').orderBy(driver_standings_df.sum_points.desc()))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

drivers_rank_window = Window.partitionBy('race_year').orderBy(desc('sum_points'), desc('wins'))

# COMMAND ----------

final_df = driver_standings_df.withColumn('rank', rank().over(drivers_rank_window))

# COMMAND ----------

display(final_df.filter('race_year == 2020'))

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')
