# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount azure data lake using service principle
# MAGIC

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-client-secret")
client_id = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-tenant-id")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlik.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlik/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlik/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlik/demo/circuits.csv", header=True))

# COMMAND ----------

#dbutils.fs.unmount("/mnt/formula1dlik/demo")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@formula1dlik.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlik/presentation",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@formula1dlik.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlik/processed",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@formula1dlik.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlik/raw",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # get secrets from key-vault
    service_credential = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-client-secret")
    client_id = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-client-id")
    tenant_id = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-tenant-id")

    # set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": service_credential,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    
    # mount storage
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())
    


# COMMAND ----------

mount_adls('formula1dlik','demo')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1dlik/raw'))

# COMMAND ----------


