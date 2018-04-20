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

//data_base_dir = os.path.join(storage_mount_path, "data", "ml-latest-small")
//val data_base_dir
val ratings = spark.sql("SELECT * FROM ratings")

// build model
val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("user_id")
  .setItemCol("movie_id")
  .setRatingCol("rating")
val model = als.fit(ratings)

// Save model
val timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance.getTime())
model.write.overwrite.save(storage_mount_path + s"/models/recommender/v_$timestamp")