# Databricks notebook source
# MAGIC %md
# MAGIC # Train Recommendation Model

# COMMAND ----------

# Import dependencies
import os
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

# Set storage mount path
storage_mount_path = "/mnt/blob_storage"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read movie ratings data

# COMMAND ----------

# Read in ratings data
ratings = spark.read.table("rating")

print("Ratings")
ratings.printSchema()
ratings.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build recommendation model using Alternating Least Square (ALS)
# MAGIC This is a type Collaborative Filtering model which lends itself to parallelization.
# MAGIC 
# MAGIC Collaborative filtering, also referred to as social filtering, filters information by using the recommendations of other people. It is based on the idea that people who agreed in their evaluation of certain items in the past are likely to agree again in the future. A person who wants to see a movie for example, might ask for recommendations from friends. The recommendations of some friends who have similar interests are trusted more than recommendations from others. This information is used in the decision on which movie to see. 
# MAGIC See here for more information: http://recommender-systems.org/collaborative-filtering/

# COMMAND ----------

# Split data into training and test
(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate recommendations

# COMMAND ----------

# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
display(userRecs)

# COMMAND ----------

# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)
display(movieRecs)

# COMMAND ----------

# Generate top 10 movie recommendations for a specified set of users
users = ratings.select(als.getUserCol()).distinct().limit(3)
userSubsetRecs = model.recommendForUserSubset(users, 10)
display(userSubsetRecs)

# COMMAND ----------

# Generate top 10 user recommendations for a specified set of movies
movies = ratings.select(als.getItemCol()).distinct().limit(3)
movieSubSetRecs = model.recommendForItemSubset(movies, 10)
display(movieSubSetRecs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save fitted model to Blob storage

# COMMAND ----------

# Save model
model.write().overwrite().save(storage_mount_path + "/models/recommender/latest")