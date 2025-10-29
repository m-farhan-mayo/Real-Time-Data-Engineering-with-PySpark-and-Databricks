# Databricks notebook source
# MAGIC %md
# MAGIC ######`Upserting the Data `

# COMMAND ----------

# MAGIC %md
# MAGIC ##`Query Source`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pyspark_cata.source.products

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.sql("SELECT * FROM pyspark_cata.source.products")

#Dedup
df = df.withColumn("dedate", row_number().over(Window.partitionBy("id").orderBy(desc("updatedDate"))))

df = df.filter(col('dedate') == 1).drop('dedate')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **`UPSERTS`**

# COMMAND ----------

#Creating Delta Object

from delta.tables import DeltaTable

if len(dbutils.fs.ls("/Volumes/pyspark_cata/source/db_volume/products_sinks/")) > 0:
    dlt_obj = DeltaTable.forPath(spark, "/Volumes/pyspark_cata/source/db_volume/products_sinks/")

    dlt_obj.alias('trg').merge(
    df.alias('src'),
    "src.id = trg.id")\
    .whenMatchedUpdateAll(condition = "src.updatedDate >= trg.updatedDate")\
    .whenNotMatchedInsertAll()\
    .execute()
    print("This Is Upserting Now!!!")

else:
    df.write.format('delta')\
        .mode("Overwrite")\
        .save("/Volumes/pyspark_cata/source/db_volume/products_sinks/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/Volumes/pyspark_cata/source/db_volume/products_sinks/`