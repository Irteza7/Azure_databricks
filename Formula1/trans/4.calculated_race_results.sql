-- Databricks notebook source
USE f1_processed


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
USING parquet
AS
SELECT
        races.race_year,
        drivers.name AS driver_name, drivers.nationality AS driver_nationality, 
        constructors.name AS team_name,
        results.position, results.points,
        11 - results.position AS relative_points

FROM f1_processed.results
JOIN f1_processed.drivers ON drivers.driver_id = results.driver_id
JOIN f1_processed.constructors ON constructors.constructor_id = results.constructor_id
JOIN f1_processed.races ON races.race_id = results.race_id

WHERE results.position <= 10



-- COMMAND ----------


