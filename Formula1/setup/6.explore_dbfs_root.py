# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore dbfs root
# MAGIC 1. list all the folders in dbfs root 
# MAGIC 2. Interact with dbfs file browser
# MAGIC 3. Upload file to dbfs root

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------


