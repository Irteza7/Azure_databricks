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

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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

results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json", multiLine=False, schema=custom_schema)
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
    .withColumn("file_date", lit(v_file_date)) \
    .drop("statusId")

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### we can also partitioned data while writing it.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# for race_id_list in results_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql("ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id={race_id_list.race_id}")

# COMMAND ----------

# results_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results;

# COMMAND ----------

df_inc_write_to_table("f1_processed","results",results_df,"race_id")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/results")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# %sql
# SELECT count(*) FROM f1_processed.results WHERE file_date="2021-04-18"


# COMMAND ----------


