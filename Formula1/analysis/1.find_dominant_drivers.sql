-- Databricks notebook source
SELECT driver_name, SUM(relative_points) AS total_points,
COUNT(driver_name) AS total_races,
AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
GROUP BY driver_name 
HAVING total_races >= 50
ORDER BY avg_points DESC
LIMIT 50;



-- COMMAND ----------

SELECT driver_name, SUM(relative_points) AS total_points,
COUNT(driver_name) AS total_races,
AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE race_year BETWEEN 2010 AND 2022
GROUP BY driver_name 
HAVING total_races >= 50
ORDER BY avg_points DESC
LIMIT 50;



-- COMMAND ----------

SELECT driver_name, SUM(relative_points) AS total_points,
COUNT(driver_name) AS total_races,
AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name 
HAVING total_races >= 50
ORDER BY avg_points DESC
LIMIT 50;



-- COMMAND ----------


