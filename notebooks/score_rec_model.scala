// Databricks notebook source
// Set storage mount path
val storage_mount_path = "/mnt/blob_storage"

// COMMAND ----------

import org.apache.spark.ml.recommendation.ALSModel
val model_base_dir = s"$storage_mount_path/models/recommender"
val model = ALSModel.read.load(model_base_dir)

// COMMAND ----------

// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)
// Generate top 10 user recommendations for each movie
val movieRecs = model.recommendForAllItems(10)

display(userRecs)

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Build connection string with the above information
val connectionString = ConnectionStringBuilder("Endpoint=sb://laceeuseh01.servicebus.windows.net/;SharedAccessKeyName=manage;SharedAccessKey=vNXVAvWNBTd8f3eyRrP+EKJm+lyOu311yUArNigjfHo=;EntityPath=laceeh02")
  .setEventHubName("laceeh02")
  .build

val customEventhubParameters =
  EventHubsConf(connectionString)
  .setMaxEventsPerTrigger(5)

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

// Event Hub message format is JSON and contains "body" field
// Body is binary, so we cast it to string to see the actual content of the message
val messages =
  incomingStream
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  .withWatermark("Time (readable)", "10 minutes")
  .select("Offset", "Time (readable)", "Timestamp", "Body")

messages.printSchema


// COMMAND ----------

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

// Define schema
val schema = StructType(Seq(
  StructField("UserId", IntegerType, true)
))

// Extract users
val users = messages
  .withColumn("UserId", from_json($"Body", schema))
  .withColumn("UserId", $"UserId.UserId")
  .select("UserId")

// Recommendations
val userRecs = model.recommendForUserSubset(users, 10)

//val userRecsTime = userRecs
//  .withColumn("Timestamp", current_timestamp)

// COMMAND ----------

var query = userRecs
//  .withWatermark("Timestamp", "10 minutes")
  .writeStream
  .outputMode("complete")
  .format("console")
  .option("truncate", false)
  .start()

query.awaitTermination()

// incomingStream.printSchema

// // Sending the incoming stream into the console.
// // Data comes in batches!
// incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

import spark.implicits._

// Generate top 10 movie recommendations for a specified set of users
val users = (List(1, 2, 3)).toDF("userId")
val userSubsetRecs = model.recommendForUserSubset(users, 10)
display(userSubsetRecs)

// COMMAND ----------

// Generate top 10 user recommendations for a specified set of movies
val movies = ratings.select(als.getItemCol).distinct().limit(3)
val movieSubSetRecs = model.recommendForItemSubset(movies, 10)