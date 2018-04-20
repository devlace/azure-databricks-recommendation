# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT movie_id, title, explode(genre) AS genre FROM movie LIMIT 100;

# COMMAND ----------

import pyspark.sql.functions as f
movie = spark.sql("SELECT * FROM movie")
movie_exp = movie.withColumn("genre", f.explode(movie.genre))

display(movie_exp
    .groupby(movie_exp.movie_id, movie_exp.title)
    .pivot("genre")
    .agg(f.count("movie_id")))



# COMMAND ----------

display(movie_exp
       .groupby(movie_exp.genre)
       .agg())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   m.title, 
# MAGIC   exp(AVG(r.rating)) AS avg_rating, 
# MAGIC   COUNT(r.rating) AS count_rating
# MAGIC FROM movie m 
# MAGIC INNER JOIN rating r 
# MAGIC ON m.movie_id = r.movie_id 
# MAGIC GROUP BY m.title 
# MAGIC HAVING count_rating > 30
# MAGIC ORDER BY avg_rating DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC # Most highly rated genres