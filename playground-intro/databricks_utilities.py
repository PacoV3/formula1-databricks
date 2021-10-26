# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

for folder in dbutils.fs.ls('/'):
    print(folder)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# Route of the notebook and time for it to respond
dbutils.notebook.run("./child_notebook", 10, {"input": "Called from main notebook"})

# COMMAND ----------

# MAGIC %pip install pandas
