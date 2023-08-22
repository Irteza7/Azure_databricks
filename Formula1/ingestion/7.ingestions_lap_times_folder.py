# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest lap_times folder
# MAGIC

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
    fields=[StructField("race_id", IntegerType(), False),
            StructField("driver_id", IntegerType(), False),
            StructField("lap", IntegerType(), True),
            StructField("position", IntegerType(), True),
            StructField("time", StringType(), True),
            StructField("milliseconds", IntegerType(), True)]
)

# COMMAND ----------

lap_times_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/lap_times/", schema=custom_schema)
# lap_times_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import lit
lap_times_df = lap_times_df.withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date)) 
lap_times_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

df_inc_write_to_table("f1_processed","lap_times",lap_times_df,"race_id")

# COMMAND ----------

# lap_times_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/lap_times")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
