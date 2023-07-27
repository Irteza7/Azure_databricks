# Databricks notebook source
# MAGIC %md
# MAGIC #### Access azure data lake using access key

# COMMAND ----------

db_access_key = dbutils.secrets.get(scope='formula1-sscope',key='formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlik.dfs.core.windows.net", db_access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlik.dfs.core.windows.net"))

# COMMAND ----------


