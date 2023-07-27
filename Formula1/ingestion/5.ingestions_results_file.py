# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest results.json file

# COMMAND ----------

# MAGIC %run "../includes/configs"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# results_df = spark.read.json(f"{raw_folder_path}/results.json", multiLine=False,)
# # results_df.show(5)

# COMMAND ----------

# schema_json = results_df.schema.json()
# ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema_json).toDDL()
# print(ddl)

# COMMAND ----------

custom_schema = "resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT, points FLOAT,laps INT, time STRING, milliseconds INT, fastestLap INT,rank INT, fastestLapTime STRING, fastestLapSpeed STRING,statusId INT"

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/results.json", multiLine=False, schema=custom_schema)
# results_df.show(5)

# COMMAND ----------

# display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, col, concat

# races_df = races_df.select(col("circuitsId")alias("circuits_id"),....)
# races_df = races_df.toDF(*new_col_list)
results_df = results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("positionText","position_text") \
    .withColumnRenamed("positionOrder","position_order") \
    .withColumnRenamed("fastestLap","fastest_lap") \
    .withColumnRenamed("fastestLapTime","fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source)) \
    .drop("statusId")

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### we can also partitioned data while writing it.
# MAGIC

# COMMAND ----------

results_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/results")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
