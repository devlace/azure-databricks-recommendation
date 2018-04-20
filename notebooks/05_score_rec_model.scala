// Databricks notebook source
// MAGIC %md
// MAGIC ## Create Movie recommendations for all users

// COMMAND ----------

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Set storage mount path
val storageMountPath = "/mnt/blob_storage"

// TODO update to capture latest versioned model
val modelVersion = "v_20180412_103448"
val modelBaseDir = s"$storageMountPath/models/recommender/$modelVersion"
val model = ALSModel.read.load(modelBaseDir)

// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)

var userRecsFlatten = userRecs
  .withColumn("recommendations", explode($"recommendations"))
  .select($"user_id", 
          $"recommendations.movie_id", 
          $"recommendations.rating", 
          lit(modelVersion).alias("model_ver_used"), 
          current_timestamp().alias("created_at"))

// Insert into table
userRecsFlatten.write.insertInto("recommendation")