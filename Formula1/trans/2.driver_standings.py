# Databricks notebook source
# MAGIC %run "../includes/configs"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, aggregate, desc, when, count
temp_race_results = race_results_df.groupBy("race_year","driver_name","driver_nationality","team").agg(sum("points").alias("total_points"), count(when(race_results_df.position == 1,True)).alias("wins")).orderBy(desc("total_points"))

# COMMAND ----------

from pyspark.sql.functions import desc, sum, count, countDistinct, rank, dense_rank
from pyspark.sql.window import Window
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_results_df = temp_race_results.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

driver_results_df.show(100)

# COMMAND ----------

driver_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------


