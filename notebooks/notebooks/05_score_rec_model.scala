// Databricks notebook source
// MAGIC %md
// MAGIC ## Create Movie recommendations for all users

// COMMAND ----------

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Set storage mount path
val storageMountPath = "/mnt/blob_storage"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load trained model

// COMMAND ----------

val modelBaseDir = s"$storageMountPath/models/recommender/latest"
val model = ALSModel.read.load(modelBaseDir)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Generate recommendations

// COMMAND ----------

// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)

var userRecsFlatten = userRecs
  .withColumn("recommendations", explode($"recommendations"))
  .select($"user_id", 
          $"recommendations.movie_id", 
          $"recommendations.rating", 
          lit("latest").alias("model_ver_used"), 
          current_timestamp().alias("created_at"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Save recommendations

// COMMAND ----------

// Insert into table
userRecsFlatten.write.insertInto("recommendation")