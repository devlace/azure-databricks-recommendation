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
# MAGIC ### Let's mix in some Spark SQL

# COMMAND ----------

df.createOrReplaceTempView("temp_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation

# COMMAND ----------

df_agg = df\
  .select("Fruit", "Color", "Price")\
  .groupBy("Color")\
  .avg("Price")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action

# COMMAND ----------

df_agg.collect()