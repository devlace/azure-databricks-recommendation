# Databricks notebook source
# Import dependencies
import os
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

# Set storage mount path
storage_mount_path = "/mnt/blob_storage"

# COMMAND ----------

# Read in ratings data
ratings = spark.read.table("rating")

print("Ratings")
ratings.printSchema()
ratings.show(5)


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
# MAGIC ## Save fitted model to Blob storage

# COMMAND ----------

# Save model
model.write().overwrite().save(storage_mount_path + "/models/recommender/latest")