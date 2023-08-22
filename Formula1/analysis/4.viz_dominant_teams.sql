-- Databricks notebook source
DROP VIEW v_dominant_teams;
CREATE TEMP VIEW v_dominant_teams
AS
SELECT team_name, 
SUM(relative_points) AS total_points,
AVG(relative_points) AS avg_points,
RANK() OVER(ORDER BY AVG(relative_points) DESC) AS team_rank
FROM f1_presentation.calculated_race_results 
GROUP BY team_name 
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC



-- COMMAND ----------

SELECT race_year, team_name, 
        SUM(relative_points) AS total_points,
        COUNT(team_name) AS total_races,
        AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE team_name IN (SELECT team_name from v_dominant_teams where team_rank <= 5)
GROUP BY race_year, team_name 
HAVING total_races >= 10
ORDER BY race_year, avg_points DESC



-- COMMAND ----------

SELECT race_year, team_name, 
        SUM(relative_points) AS total_points,
        COUNT(team_name) AS total_races,
        AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE team_name IN (SELECT team_name from v_dominant_teams where team_rank <= 5)
GROUP BY race_year, team_name 
HAVING total_races >= 10
ORDER BY race_year, avg_points DESC



-- COMMAND ----------

SELECT race_year, team_name, 
        SUM(relative_points) AS total_points,
        COUNT(team_name) AS total_races,
        AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE team_name IN (SELECT team_name from v_dominant_teams where team_rank <= 5)
GROUP BY race_year, team_name 
HAVING total_races >= 10
ORDER BY race_year, avg_points DESC



-- COMMAND ----------


