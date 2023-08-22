# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest pit_stops.json file

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

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, TimestampType, DateType

custom_schema = StructType(
    fields=[StructField("raceId", IntegerType(), False),
            StructField("driverId", IntegerType(), False),
            StructField("stop", IntegerType(), True),
            StructField("lap", IntegerType(), True),
            StructField("time", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("milliseconds", IntegerType(), True)]
)

# COMMAND ----------

pit_stops_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json", multiLine=True, schema=custom_schema)
# pit_stops_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, col, concat

# races_df = races_df.select(col("circuitsId")alias("circuits_id"),....)
# races_df = races_df.toDF(*new_col_list)
pit_stops_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

pit_stops_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

df_inc_write_to_table("f1_processed","pit_stops",pit_stops_df,"race_id")

# COMMAND ----------

# pit_stops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/pit_stops")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
