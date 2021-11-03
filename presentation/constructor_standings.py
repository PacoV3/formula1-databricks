# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-04-18')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

base_race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

race_results_list = base_race_results_df \
    .filter(f"file_date = '{v_file_date}'") \
    .select('race_year') \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list = list(map(lambda x: x.race_year, race_results_list))

# COMMAND ----------

race_results_df = base_race_results_df.filter(base_race_results_df.race_year.isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, when

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy('race_year', 'team') \
    .agg(count(when(race_results_df.position == 1, True)).alias('wins'),
        sum('points').alias('sum_points'),)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_window = Window.partitionBy('race_year').orderBy(desc('sum_points'), desc('wins'))

# COMMAND ----------

final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_rank_window))

# COMMAND ----------

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')
incremental_load(input_df = final_df, partition_column = 'race_year', db ='f1_presentation', table = 'constructor_standings')
