# Databricks notebook source
# MAGIC %md
# MAGIC #### Access azure data lake using SAS
# MAGIC

# COMMAND ----------

sas_credential = dbutils.secrets.get(scope="formula1-sscope",key="formula1dl-demo-sas")


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlik.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlik.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlik.dfs.core.windows.net", sas_credential)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlik.dfs.core.windows.net"))

# COMMAND ----------


