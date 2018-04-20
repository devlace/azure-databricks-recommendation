// Databricks notebook source
// MAGIC %md
// MAGIC ## Retrain Recommendation model

// COMMAND ----------

// Import dependencies
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import java.util.Calendar
import java.text.SimpleDateFormat

// Set storage mount path
val storage_mount_path = "/mnt/blob_storage"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read in ratings data

// COMMAND ----------

// Read in ratings
val ratings = spark.read.table("rating")
display(ratings)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Re-fit ALS model

// COMMAND ----------

val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("user_id")
  .setItemCol("movie_id")
  .setRatingCol("rating")
val model = als.fit(ratings)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Save model

// COMMAND ----------

model.write.overwrite.save(storage_mount_path + s"/models/recommender/latest")