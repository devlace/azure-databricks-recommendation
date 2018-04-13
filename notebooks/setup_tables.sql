-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.fs.refreshMounts()
-- MAGIC // WARNING! This will delete data
-- MAGIC dbutils.fs.rm("/mnt/blob_storage/data/rating", true)
-- MAGIC dbutils.fs.rm("/mnt/blob_storage/data/recommendation", true)

-- COMMAND ----------

-- Create table
DROP TABLE IF EXISTS rating;
CREATE TABLE rating
(
  user_id int,
  movie_id int,
  rating double,
  created_at timestamp,
  year string
)
USING org.apache.spark.sql.parquet
PARTITIONED BY (year)
LOCATION '/mnt/blob_storage/data/rating/files/';

-- Insert data into table
DROP TABLE IF EXISTS rating_temp;
CREATE TABLE rating_temp
(
  user_id int,
  movie_id int,
  rating double,
  `timestamp` long
)
USING CSV
LOCATION '/mnt/blob_storage/rawdata/ml-latest-small/ratings.csv';

INSERT INTO rating
SELECT 
  user_id,
  movie_id,
  rating,
  CAST(`timestamp` AS timestamp) AS created_at,
  from_unixtime(`timestamp`, 'yyyy') AS year
FROM rating_temp
WHERE user_id IS NOT NULL;

-- Drop temporary table
DROP TABLE rating_temp;

-- Refresh
REFRESH TABLE rating;

SELECT * FROM rating LIMIT 100;

-- COMMAND ----------

-- Create table
DROP TABLE IF EXISTS recommendation;
CREATE TABLE recommendation
(
  user_id int,
  movie_id int,
  rating double,
  model_version string,
  created_at timestamp
)
USING org.apache.spark.sql.parquet
LOCATION '/mnt/blob_storage/data/recommendation/files/';
