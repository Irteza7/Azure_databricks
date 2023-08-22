# Databricks notebook source
# MAGIC %run "../includes/configs"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date='{v_file_date}'")\
    .select("race_year")\
    .distinct()\
    .collect()

race_year_list = [race_year.race_year for race_year in race_results_list]

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

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

df_inc_write_to_table("f1_presentation","constructor_standings",constructor_results_df,"race_year")

# COMMAND ----------

# constructor_results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------


