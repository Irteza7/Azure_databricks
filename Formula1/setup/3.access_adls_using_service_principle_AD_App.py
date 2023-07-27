# Databricks notebook source
# MAGIC %md
# MAGIC #### Access azure data lake using service principle
# MAGIC

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-client-secret")
client_id = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-sscope",  key="formula1-app-tenant-id")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlik.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlik.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlik.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlik.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlik.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlik.dfs.core.windows.net"))

# COMMAND ----------


