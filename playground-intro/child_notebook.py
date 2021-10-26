# Databricks notebook source
dbutils.widgets.text("input", "", "Send the parameter value")

# COMMAND ----------

input_param = dbutils.widgets.get("input")

# COMMAND ----------

print(f'The input parameter is "{input_param}"')

# COMMAND ----------

dbutils.notebook.exit(100)
