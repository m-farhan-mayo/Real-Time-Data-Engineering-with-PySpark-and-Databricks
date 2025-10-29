# Databricks notebook source
# MAGIC %md
# MAGIC ###### `The Scenario In which we have json data and we just flattered it.`

# COMMAND ----------

df = spark.read.format("json")\
    .option("inferSchema", True)\
    .option("multiLine", True)\
    .load("/Volumes/pyspark_cata/source/db_volume/jsondata/")


display(df)

# COMMAND ----------

df.schema

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_cust = df.select("customer.customer_id","customer.email","customer.location.city","customer.location.country","*").drop("customer")

df_cust_new = df_cust.withColumn("delivery_updates", explode("delivery_updates"))\
                     .withColumn("items", explode("items"))\
                     .select("*")
display(df_cust_new)


# COMMAND ----------


