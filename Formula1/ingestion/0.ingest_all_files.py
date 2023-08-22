# Databricks notebook source
v_result = dbutils.notebook.run("1.ingestions_circuits_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingestions_races_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingestions_constructor_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingestions_drivers_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("5.ingestions_results_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("6.ingestions_pitstops_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("7.ingestions_lap_times_folder", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})


# COMMAND ----------

v_result = dbutils.notebook.run("8.ingestions_qualifying_folder", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})


# COMMAND ----------

v_result
