# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

full_load_notebooks = ['ingest_circuits_csv', 'ingest_constructors_json', 'ingest_drivers_json', 'ingest_races_csv']
incremental_load_notebooks = ['ingest_lap_times_csv_folder', 'ingest_pitstops_json', 'ingest_qualifying_json_folder', 'ingest_results_json']
file_dates = ['2021-03-21', '2021-03-28', '2021-04-18']

# COMMAND ----------

# Run all of them one at a time
def run_full_load_notebooks(notebooks):
    for notebook in notebooks:
        result = dbutils.notebook.run(notebook, 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-03-21'})
        if result != 'Success':
            return f'Error in the execution of the {notebook} notebook'
    return 'Success in all notebooks'

# COMMAND ----------

# Run all of them one at a time
def run_incremental_load_notebooks(notebooks, file_dates):
    for notebook in notebooks:
        for file_date in file_dates:
            result = dbutils.notebook.run(notebook, 0, {'p_data_source': 'Ergast API', 'p_file_date': file_date})
            if result != 'Success':
                return f'Error in the execution of the {notebook} notebook'
    return 'Success in all notebooks'

# COMMAND ----------

run_full_load_notebooks(full_load_notebooks)

# COMMAND ----------

run_incremental_load_notebooks(incremental_load_notebooks, file_dates)
