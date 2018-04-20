# Databricks notebook source
# MAGIC %md
# MAGIC ## Spark session

# COMMAND ----------

spark

# COMMAND ----------

spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Dataframe

# COMMAND ----------

df = spark.createDataFrame([('Fiji Apple', 'Red', 3.5), 
                           ('Banana', 'Yellow', 1.0),
                           ('Green Grape', 'Green', 2.0),
                           ('Red Grape', 'Red', 2.0),
                           ('Peach', 'Yellow', 3.0),
                           ('Orange', 'Orange', 2.0),
                           ('Green Apple', 'Green', 2.5)], 
                           ['Fruit', 'Color', 'Price'])
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation

# COMMAND ----------

df_agg = df
  .select("Fruit", "Color", "Price")
  .groupBy("Color")
  .avg("Price")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action

# COMMAND ----------

df_agg.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## List sample databricks datasets using dbutils

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /databricks-datasets/tpch/data-001/lineitem

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([StructField("date", LongType(), True),
                     StructField("delay", IntegerType(), True),
                     StructField("distance", IntegerType(), True),
                     StructField("origin", StringType(), True),
                     StructField("destination", StringType(), True)])

delays_df = spark.read.format("csv")\
  .option("schema", schema)\
  .load("/databricks-datasets/flights")

# COMMAND ----------

delays_df.count()

# COMMAND ----------

from pyspark.sql.types import *

lineitem_df = spark.read.format("csv")\
  .option("delimiter", "|")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/databricks-datasets/tpch/data-001/lineitem")
  
display(lineitem_df)

# COMMAND ----------

display(delays_df)

# COMMAND ----------

delays_df.count()

# COMMAND ----------

iot_df = spark.read.format("json")\
  .option("inferSchema", "true")\
  .load("dbfs:/databricks-datasets/iot/iot_devices.json")
  
iot_df.schema

# COMMAND ----------

iot_df.count()