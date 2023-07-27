# Databricks notebook source
# MAGIC %run "../includes/configs"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, aggregate, desc, when, count
temp_race_results = race_results_df.groupBy("race_year","team").agg(sum("points").alias("total_points"), count(when(race_results_df.position == 1,True)).alias("wins")).orderBy(desc("total_points"))

# COMMAND ----------

temp_race_results.show(100)

# COMMAND ----------

from pyspark.sql.functions import desc, sum, count, countDistinct, rank, dense_rank
from pyspark.sql.window import Window
constructorRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
constructor_results_df = temp_race_results.withColumn("rank", rank().over(constructorRankSpec))

# COMMAND ----------

constructor_results_df.show(100)

# COMMAND ----------

constructor_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


