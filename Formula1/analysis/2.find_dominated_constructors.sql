-- Databricks notebook source
SELECT team_name, SUM(relative_points) AS total_points,
COUNT(team_name) AS total_participation,
AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
GROUP BY team_name 
HAVING total_participation >= 100
ORDER BY avg_points DESC
LIMIT 50;



-- COMMAND ----------

SELECT team_name, SUM(relative_points) AS total_points,
COUNT(team_name) AS total_participation,
AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name 
HAVING total_participation >= 50
ORDER BY avg_points DESC
LIMIT 50;



-- COMMAND ----------

SELECT team_name, SUM(relative_points) AS total_points,
COUNT(team_name) AS total_participation,
AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name 
HAVING total_participation >= 50
ORDER BY avg_points DESC
LIMIT 50;



-- COMMAND ----------


