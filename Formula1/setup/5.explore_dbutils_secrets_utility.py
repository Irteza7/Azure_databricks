# Databricks notebook source
### Explore cabapilities of dbutils.secrets utility


# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-sscope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-sscope',key='formula1dl-account-key')

# COMMAND ----------

import os

# COMMAND ----------

os.getcwd()

# COMMAND ----------


