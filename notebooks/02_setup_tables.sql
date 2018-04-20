-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.fs.refreshMounts()
-- MAGIC 
-- MAGIC // WARNING! This will delete data
-- MAGIC dbutils.fs.rm("/mnt/blob_storage/data/rating", true)
-- MAGIC dbutils.fs.rm("/mnt/blob_storage/data/recommendation", true)
-- MAGIC dbutils.fs.rm("/mnt/blob_storage/data/movie", true)
-- MAGIC dbutils.fs.rm("/mnt/blob_storage/data/tag", true)
-- MAGIC dbutils.fs.rm("/mnt/blob_storage/data/link", true)

-- COMMAND ----------

-- Ratings
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

-- Create temp table
DROP TABLE IF EXISTS rating_temp;
CREATE TABLE rating_temp
(
  user_id int,
  movie_id int,
  rating double,
  `timestamp` long
)
USING CSV
LOCATION '/mnt/blob_storage/rawdata/ml-latest-small/ratings.csv'
OPTIONS ("header"="true");

-- Insert data into table
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

-- Recommendation
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

SELECT * FROM recommendation;

-- COMMAND ----------

-- Movie
DROP TABLE IF EXISTS movie;
CREATE TABLE movie
(
  movie_id int,
  title string,
  genre array<string>
)
USING org.apache.spark.sql.parquet
LOCATION '/mnt/blob_storage/data/movie/files/';

-- Create temp table
DROP TABLE IF EXISTS movies_temp;
CREATE TABLE movies_temp
(
  movie_id int,
  title string,
  genre string
)
USING CSV
LOCATION '/mnt/blob_storage/rawdata/ml-latest-small/movies.csv'
OPTIONS ("header"="true");

INSERT INTO movie
SELECT 
  movie_id,
  title,
  split(genre, "\\|") AS genre
FROM movies_temp
WHERE movie_id IS NOT NULL;

-- Drop temporary table
DROP TABLE movies_temp;

-- Refresh
REFRESH TABLE movie;

SELECT * FROM movie LIMIT 100;

-- COMMAND ----------

-- Tags
DROP TABLE IF EXISTS tag;
CREATE TABLE tag
(
  user_id int,
  movie_id int,
  tag string,
  created_at timestamp
)
USING org.apache.spark.sql.parquet
LOCATION '/mnt/blob_storage/data/tag/files/';

-- Create temp table
DROP TABLE IF EXISTS tag_temp;
CREATE TABLE tag_temp
(
  user_id int,
  movie_id int,
  tag string,
  `timestamp` long
)
USING CSV
LOCATION '/mnt/blob_storage/rawdata/ml-latest-small/tags.csv'
OPTIONS ("header"="true");

INSERT INTO tag
SELECT 
  user_id,
  movie_id,
  tag,
  CAST(`timestamp` AS timestamp) AS created_at
FROM tag_temp
WHERE user_id IS NOT NULL;

-- Drop temporary table
DROP TABLE tag_temp;

-- Refresh
REFRESH TABLE tag;

SELECT * FROM tag LIMIT 100;

-- COMMAND ----------

-- Link
DROP TABLE IF EXISTS link;
CREATE TABLE link
(
  movie_id int,
  imdb_id int,
  tmdb_id int
)
USING org.apache.spark.sql.parquet
LOCATION '/mnt/blob_storage/data/link/files/';

-- Create temp table
DROP TABLE IF EXISTS link_temp;
CREATE TABLE link_temp
(
  movie_id int,
  imdb_id int,
  tmdb_id int
)
USING CSV
LOCATION '/mnt/blob_storage/rawdata/ml-latest-small/links.csv'
OPTIONS ("header"="true");

INSERT INTO link
SELECT movie_id, imdb_id, tmdb_id
FROM link_temp
WHERE movie_id IS NOT NULL;

-- Drop temporary table
DROP TABLE link_temp;

-- Refresh
REFRESH TABLE link;

SELECT * FROM link LIMIT 100;