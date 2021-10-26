# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

secret_scope = 'formula1-scope'
storage_account_name = "formula1dludemy"
client_id            = dbutils.secrets.get(scope=secret_scope, key='databricks-app-client-id')
tenant_id            = dbutils.secrets.get(scope=secret_scope, key='databricks-app-tenant-id')
client_secret        = dbutils.secrets.get(scope=secret_scope, key='databricks-app-client-secret')

# COMMAND ----------

configs = {'fs.azure.account.auth.type': 'OAuth',
           'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
           'fs.azure.account.oauth2.client.id': f'{client_id},
           'fs.azure.account.oauth2.client.secret': f'{client_secret},
           'fs.azure.account.oauth2.client.endpoint': f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'}

# COMMAND ----------

def mount_a_dls(container_name):
    dbutils.fs.mount(
        source = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/,
        mount_point = f'/mnt/{storage_account_name}/{container_name},
        extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dludemy/raw')
dbutils.fs.unmount('/mnt/formula1dludemy/processed')
dbutils.fs.unmount('/mnt/formula1dludemy/presentation')

# COMMAND ----------

mount_a_dls('raw')

# COMMAND ----------

mount_a_dls('processed')

# COMMAND ----------

mount_a_dls('presentation')

# COMMAND ----------

for mount in dbutils.fs.mounts():
    if any(container in mount.mountPoint for container in {'processed', 'raw', 'presentation'}):
        print(mount)
