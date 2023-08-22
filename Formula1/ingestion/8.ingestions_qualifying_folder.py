# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest qualifying folder
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
    fields=[StructField("qualifyId", IntegerType(), False),
            StructField("raceId", IntegerType(), False),
            StructField("driverId", IntegerType(), False),
            StructField("constructorId", IntegerType(), False),
            StructField("number", IntegerType(), True),
            StructField("position", IntegerType(), True),
            StructField("q1", StringType(), True),
            StructField("q2", StringType(), True),
            StructField("q3", StringType(), True)]
)

# COMMAND ----------

qualifying_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/qualifying/", multiLine = True, schema=custom_schema)
# qualifying_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, col, concat

# races_df = races_df.select(col("circuitsId")alias("circuits_id"),....)
# races_df = races_df.toDF(*new_col_list)
qualifying_df = qualifying_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

qualifying_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

df_inc_write_to_table("f1_processed","qualifying",qualifying_df,"race_id")

# COMMAND ----------

# qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/qualifying")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
