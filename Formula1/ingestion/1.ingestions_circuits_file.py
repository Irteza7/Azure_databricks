# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configs"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, TimestampType

custom_schema = StructType(
    fields=[StructField("circuitId", IntegerType(), False),
            StructField("circuitRef", StringType(), True),
            StructField("name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("country", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
            StructField("alt", IntegerType(), True),
            StructField("url", StringType(), True)]
)

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv" , header=True, schema=custom_schema)
# circuits_df.show(5)

# COMMAND ----------

circuits_df = circuits_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import col, lit
# circuits_df = circuits_df.select(col("circuitsId")alias("circuits_id"),....)
# circuits_df = circuits_df.toDF(*new_col_list)
circuits_df = circuits_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

circuits_df = add_ingestion_date(circuits_df)

# COMMAND ----------

circuits_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
