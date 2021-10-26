# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

ingestion_notebooks = ['ingest_circuits_csv', 'ingest_constructors_json', 'ingest_drivers_json','ingest_lap_times_csv_folder', 
                   'ingest_pitstops_json', 'ingest_qualifying_json_folder', 'ingest_races_csv', 'ingest_results_json']

# COMMAND ----------

# Run all of them one at a time
def run_notebooks(notebooks):
    for notebook in notebooks:
        result = dbutils.notebook.run(notebook, 0, {'p_data_source': 'Ergast API'})
        if result != 'Success':
            return f'Error in the execution of the {notebook} notebook'
    return 'Success in all notebooks'

# COMMAND ----------

run_notebooks(ingestion_notebooks)
