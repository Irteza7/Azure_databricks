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

lap_times_df = spark.read.csv(f"{raw_folder_path}/lap_times/", schema=custom_schema)
# lap_times_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import lit
lap_times_df = lap_times_df.withColumn("data_source", lit(v_data_source))
lap_times_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/lap_times")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
