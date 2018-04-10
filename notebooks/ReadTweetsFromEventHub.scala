// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Build connection string with the above information
val connectionString = ConnectionStringBuilder("Endpoint=sb://laceeuseh01.servicebus.windows.net/;SharedAccessKeyName=sasmanage;SharedAccessKey=dbbb9zwuTKTOo8Vx/CnPLgRQV0IpgJJTpT+f/NsMJ4s=;EntityPath=laceeh01")
  .setEventHubName("laceeh01")
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
  .select("Offset", "Time (readable)", "Timestamp", "Body")

messages.printSchema

messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// incomingStream.printSchema

// // Sending the incoming stream into the console.
// // Data comes in batches!
// incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------



