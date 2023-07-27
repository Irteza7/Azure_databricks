# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest races.csv file

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
    fields=[StructField("raceId", IntegerType(), False),
            StructField("year", IntegerType(), True),
            StructField("round", IntegerType(), True),
            StructField("circuitId", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", DateType(), True),
            StructField("time", StringType(), True),
            StructField("url", StringType(), True)]
)

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/races.csv", header=True, schema=custom_schema)
races_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, col, concat

# races_df = races_df.select(col("circuitsId")alias("circuits_id"),....)
# races_df = races_df.toDF(*new_col_list)
races_df = races_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("data_source", lit(v_data_source)) \
    .drop("date","time", "url")

# COMMAND ----------

races_df = add_ingestion_date(races_df)

# COMMAND ----------

races_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/races")
# display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### we can also partitioned data while writing it.
# MAGIC

# COMMAND ----------

# races_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1dlik/processed/races")

# COMMAND ----------

dbutils.notebook.exit("Success")
