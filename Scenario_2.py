# Databricks notebook source
# MAGIC %md
# MAGIC ######`Batch Mode Just For Development Purpose To Check The Data Is Fine Or Not !`

# COMMAND ----------

my_schema = """
            order_id INT,
            customer_id INT,
            order_date DATE,
            amount DOUBLE

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Streaming Query`

# COMMAND ----------

#Batch Mode Just For Development Purpose To Check The Data Is Fine Or Not !
df_Batch = spark.read.format("csv")\
        .option('header', "true")\
        .schema(my_schema)\
        .load("/Volumes/pyspark_cata/source/streamsource/")

display(df_Batch)



# COMMAND ----------

df = spark.readStream.format("csv")\
        .option('header', "true")\
        .schema(my_schema)\
        .load("/Volumes/pyspark_cata/source/streamsource/")



# COMMAND ----------

# MAGIC %md
# MAGIC ## `Streaming Output`

# COMMAND ----------

df.writeStream.format("delta")\
  .option("checkpointLocation", "/Volumes/pyspark_cata/source/streamsink/checkpoint")\
  .option("mergeSchema", "true")\
  .trigger(once = True)\
  .start("/Volumes/pyspark_cata/source/streamsink/data")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/pyspark_cata/source/streamsink/data`

# COMMAND ----------

