# Databricks notebook source
# Create widget
dbutils.widgets.remove("user_id")
user_ids = spark.read.table("rating").select("user_id").distinct().toPandas()
user_ids_list = user_ids["user_id"].tolist()
dbutils.widgets.dropdown("user_id", str(user_ids_list[0]), [str(x) for x in user_ids_list])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Movie recommendations

# COMMAND ----------

import pyspark.sql.functions as f

recs = spark.read.table("recommendation")
movies = spark.read.table("movie")

user_recs = recs.filter(recs.user_id == dbutils.widgets.get("user_id"))\
  .join(movies, on="movie_id")\
  .groupBy(recs.movie_id, movies.title)\
  .agg(f.avg(recs.rating).alias("avg_rating"))
  
top_user_recs = user_recs.orderBy(user_recs.avg_rating.desc()).limit(10)
top_user_recs = top_user_recs.cache()
display(top_user_recs)

# COMMAND ----------

display(top_user_recs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Genres based on recommendations

# COMMAND ----------

import pyspark.sql.functions as f
movie_exp = movies.withColumn("genre", f.explode(movies.genre))

user_recs_genre = recs.filter(recs.user_id == dbutils.widgets.get("user_id"))\
  .join(movie_exp, on="movie_id")\
  .groupBy(movie_exp.genre)\
  .agg(f.avg(recs.rating).alias("avg_rating"))
  
top_user_recs_genre = user_recs_genre.orderBy(user_recs_genre.avg_rating.desc()).cache()
display(top_user_recs_genre)


# COMMAND ----------

display(top_user_recs_genre)
