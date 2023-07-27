# Databricks notebook source
# MAGIC %run "../includes/configs"
# MAGIC

# COMMAND ----------

"""race_year - races

race_name - races

race_date - races

circuit_location - circuit

driver_name - drivers

driver_number - drivers

driver_nationality - drivers

team - constructors

grid - results

fastest_lap - results

race_time - results

points - results

created_date - current_timestamp"""

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
constructors_df =  spark.read.parquet(f"{processed_folder_path}/constructor")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.name.alias("race_name"), races_df.race_timestamp.alias("race_date"), circuits_df.location.alias("circuit_location"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

race_results_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")\
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")\
    .join(races_circuits_df, results_df.race_id == races_circuits_df.race_id, "inner") \
    .select(races_circuits_df.race_year, races_circuits_df.race_name, races_circuits_df.race_date, races_circuits_df.circuit_location, drivers_df.name.alias("driver_name"), drivers_df.number.alias("driver_number"), drivers_df.nationality.alias("driver_nationality"), constructors_df.name.alias("team"), results_df.grid, results_df.fastest_lap, results_df.time.alias("race_time"), results_df.points, results_df.position).withColumn("created_date", current_timestamp())

# COMMAND ----------

display(race_results_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'" ).orderBy(race_results_df.points.desc()))

# COMMAND ----------

race_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, aggregate, desc
race_results_df.groupBy("driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races")).orderBy(desc("total_points")).show(10)

# COMMAND ----------

race_results_df.show(10)

# COMMAND ----------


