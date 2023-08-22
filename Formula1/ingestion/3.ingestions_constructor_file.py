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

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

custom_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema=custom_schema)
constructors_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, col, concat

# constructor_df = constructor_df.select(col("circuitsId")alias("circuits_id"),....)
# constructor_df = constructor_df.toDF(*new_col_list)
constructors_df = constructors_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date)) \
    .drop("url")

# COMMAND ----------

constructors_df = add_ingestion_date(constructors_df)

# COMMAND ----------

constructors_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/constructors")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
