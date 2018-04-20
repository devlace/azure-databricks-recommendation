// Databricks notebook source
// MAGIC %md
// MAGIC ## Retrieve storage credentials

// COMMAND ----------

val eventhubNamespace = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_namespace")
val eventhubRatings = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_ratings")
val eventhubRatingKey = dbutils.preview.secret.get(scope = "storage_scope", key = "eventhub_ratings_key")

// Set storage mount path
val storage_mount_path = "/mnt/blob_storage"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingest Data from Event Hubs

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Build connection string with the above information
val connectionString = ConnectionStringBuilder(s"Endpoint=sb://$eventhubNamespace.servicebus.windows.net/;SharedAccessKeyName=manage;SharedAccessKey=$eventhubRatingKey;EntityPath=$eventhubRatings")
  .setEventHubName(eventhubRatings)
  .build

val customEventhubParameters =
  EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

val incomingStream = spark
  .readStream
  .format("eventhubs")
  .options(customEventhubParameters.toMap)
  .load()

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

var messageTransformed = 
  messages
  .select(get_json_object($"Body", "$.UserId").cast(IntegerType).alias("user_id"),
          get_json_object($"Body", "$.MovieId").cast(IntegerType).alias("movie_id"), 
          get_json_object($"Body", "$.Rating").cast(DoubleType).alias("rating"), 
          get_json_object($"Body", "$.Timestamp").cast(LongType).cast(TimestampType).alias("created_at"),
          from_unixtime(get_json_object($"Body", "$.Timestamp"), "yyyy").alias("year"))

// Output
// var query = messageTransformed
//   .writeStream
//   .outputMode("append")
//   .format("console")
//   .option("truncate", false)
//   .start()
// query.awaitTermination()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Export data to Spark Table (Blob Storage)

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val query =
  messageTransformed
    .writeStream
    .format("parquet")
    .option("path", s"$storage_mount_path/data/rating/files/")
    .option("checkpointLocation", s"$storage_mount_path/data/rating/check/")
    .partitionBy("year")
    .trigger(ProcessingTime("10 seconds"))
    .start()