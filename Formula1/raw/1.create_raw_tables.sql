-- Databricks notebook source
-- MAGIC %run "../includes/configs"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitid INT, circuitRef STRING, name STRING, location STRING, country STRING, lat DOUBLE, lng DOUBLE, alt INT, url STRING)
USING csv
OPTIONS (path "/mnt/formula1dlik/raw/circuits.csv", header True)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits LIMIT 10

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races (raceld INT, year INT, round INT, circuitId INT, name STRING, date DATE, time STRING, url STRING)

USING csv

OPTIONS (path "/mnt/formula1dlik/raw/races.csv", header True)

-- COMMAND ----------

SELECT * FROM f1_raw.races LIMIT 10;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors ( constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING)
USING JSON
OPTIONS (path "/mnt/formula1dlik/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors LIMIT 10;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers( driverId INT, 
driverRef STRING, 
number INT, 
code STRING, 
name STRUCT<forename: STRING, 
surname: STRING>, 
dob DATE, 
nationality STRING, 
url STRING)

USING json

OPTIONS (path "/mnt/formula1dlik/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results (resultId INT, raceId INT, 
driverId INT, constructorId INT, number INT, 
grid INT, position INT, positionText STRING, 
positionOrder INT, points INT,
Laps INT, time STRING, milliseconds INT, 
fastestLap INT, rank INT, fastestLapTime STRING, 
fastestLapSpeed FLOAT, statusId STRING)

USING json
OPTIONS (path "/mnt/formula1dlik/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results LIMIT 10;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (driverId INT, duration STRING,
lap INT, milliseconds INT, 
raceId INT, stop INT, time STRING)

USING json

OPTIONS (path "/mnt/formula1dlik/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops LIMIT 10;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times (raceld INT, driverId INT,
lap INT, position INT, time STRING, milliseconds INT)

USING csv
OPTIONS (path "/mnt/formula1dlik/raw/lap_times")

-- COMMAND ----------

SELECT count(*) FROM f1_raw.lap_times;

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times LIMIT 10;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(constructorId INT, driverId INT, 
number INT, position INT, 
q1 STRING, q2 STRING,  
q3 STRING, qualifyId INT, raceId INT)

USING json

OPTIONS (path "/mnt/formula1dlik/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT count(*) FROM f1_raw.qualifying;

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying LIMIT 10;

-- COMMAND ----------


