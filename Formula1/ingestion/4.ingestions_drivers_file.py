# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest drivers.json file

# COMMAND ----------

# MAGIC %run "../includes/configs"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType, TimestampType, DateType

name_schema = StructType(fields=[StructField("forename", StringType(), True), StructField("surname", StringType(), True)])
custom_schema = StructType(
    fields=[StructField("driverId", IntegerType(), False),
            StructField("driverRef", StringType(), True),
            StructField("number", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("name", name_schema, True),
            StructField("dob", DateType(), True),
            StructField("nationality", StringType(), True),
            StructField("url", StringType(), True)]
)

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/drivers.json", schema=custom_schema)
drivers_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, col, concat

# races_df = races_df.select(col("circuitsId")alias("circuits_id"),....)
# races_df = races_df.toDF(*new_col_list)
drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
    .withColumn("data_source", lit(v_data_source)) \
    .drop("url")

# COMMAND ----------

drivers_df = add_ingestion_date(drivers_df)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/drivers")
# display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
