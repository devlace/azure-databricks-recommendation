
# COMMAND ----------

storage_account = dbutils.preview.secret.get(scope = "storage_scope", key = "storageAccount")
storage_key = dbutils.preview.secret.get(scope = "storage_scope", key = "storageKey")
spark.conf.set("fs.azure.account.key." + storage_account + ".blob.core.windows.net", storage_key)

# COMMAND ----------

import os
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

# COMMAND ----------

base_dir = os.path.join("wasbs://data@" + storage_account + ".blob.core.windows.net", "ml-latest-small")

links = spark.read.csv(os.path.join(base_dir, "links.csv"), header=True, inferSchema=True)
movies = spark.read.csv(os.path.join(base_dir, "movies.csv"), header=True, inferSchema=True)
ratings = spark.read.csv(os.path.join(base_dir, "ratings.csv"), header=True, inferSchema=True)
tags = spark.read.csv(os.path.join(base_dir, "tags.csv"), header=True, inferSchema=True)

print("Links")
links.printSchema()
links.show(5)

print("Movies")
movies.printSchema()
movies.show(5)

print("Ratings")
ratings.printSchema()
ratings.show(5)

print("Tags")
tags.printSchema()
tags.show(5)


# COMMAND ----------

# Split
(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)

display(predictions)

# COMMAND ----------

# Evaluate Model
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))


# COMMAND ----------

# Save model
model.save("")

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