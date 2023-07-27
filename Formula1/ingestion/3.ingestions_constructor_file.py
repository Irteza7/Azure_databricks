# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest constructor.json file

# COMMAND ----------

# MAGIC %run "../includes/configs"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

custom_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema=custom_schema)
constructor_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, col, concat

# constructor_df = constructor_df.select(col("circuitsId")alias("circuits_id"),....)
# constructor_df = constructor_df.toDF(*new_col_list)
constructor_df = constructor_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .withColumn("data_source", lit(v_data_source)) \
    .drop("url")

# COMMAND ----------

constructor_df = add_ingestion_date(constructor_df)

# COMMAND ----------

constructor_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructor")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/constructor")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
