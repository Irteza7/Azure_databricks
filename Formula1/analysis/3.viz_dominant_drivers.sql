-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Forumla 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name, SUM(relative_points) AS total_points,
COUNT(driver_name) AS total_races,
AVG(relative_points) AS avg_points,
RANK() OVER(ORDER BY AVG(relative_points) DESC) AS driver_rank
FROM f1_presentation.calculated_race_results 
GROUP BY driver_name 
HAVING total_races >= 50
ORDER BY avg_points DESC



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

SELECT race_year, driver_name, 
        SUM(relative_points) AS total_points,
        COUNT(driver_name) AS total_races,
        AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name 
HAVING total_races >= 10
ORDER BY race_year, avg_points DESC



-- COMMAND ----------

-- CTE
WITH cte_dominant_drivers AS (
    SELECT driver_name, 
           SUM(relative_points) AS total_points,
           COUNT(driver_name) AS total_races,
           AVG(relative_points) AS avg_points,
           RANK() OVER (ORDER BY AVG(relative_points) DESC) AS driver_rank
    FROM f1_presentation.calculated_race_results 
    GROUP BY driver_name 
    HAVING total_races >= 50
)

SELECT race_year, 
       driver_name, 
       SUM(relative_points) AS total_points,
       COUNT(driver_name) AS total_races,
       AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE driver_name IN (SELECT driver_name from cte_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name 
HAVING total_races >= 10
ORDER BY race_year, avg_points DESC


-- COMMAND ----------

SELECT race_year, 
       driver_name, 
       SUM(relative_points) AS total_points,
       COUNT(driver_name) AS total_races,
       AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name 
HAVING total_races >= 10
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year, 
       driver_name, 
       SUM(relative_points) AS total_points,
       COUNT(driver_name) AS total_races,
       AVG(relative_points) AS avg_points
FROM f1_presentation.calculated_race_results 
WHERE driver_name IN (SELECT driver_name from v_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name 
HAVING total_races >= 10
ORDER BY race_year, avg_points DESC

-- COMMAND ----------


