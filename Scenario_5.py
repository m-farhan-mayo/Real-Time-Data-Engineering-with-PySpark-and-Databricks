# Databricks notebook source
# MAGIC %md
# MAGIC ###### `Slowly Changing Dimentions Dynamically`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE pyspark_cata.source.customers
# MAGIC (
# MAGIC   id INT,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   modifiedDate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO pyspark_cata.source.customers (id, email, city, country, modifiedDate) VALUES
# MAGIC (1, 'john.doe@example.com', 'New Jersery', 'USA', current_timestamp()),
# MAGIC (6, 'jane.smith@example.com', 'Los Angeles', 'USA',current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM pyspark_cata.source.customers

# COMMAND ----------

if spark.catalog.tableExists("pyspark_cata.source.DimCustomers"):

    pass

else:
    spark.sql("""
              CREATE TABLE pyspark_cata.source.DimCustomers
              SELECT * ,
              current_timestamp() as start_Time,
              CAST('3000-01-01' AS TIMESTAMP) as end_Time,
              "Y" as isActive
              FROM pyspark_cata.source.customers
              
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pyspark_cata.source.DimCustomers

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## `SCD TYPE-2`

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import *

df = spark.sql("""
               SELECT * FROM pyspark_cata.source.customers
               """)

df = df.withColumn("Dedup", row_number().over(Window.partitionBy("id").orderBy(desc("modifiedDate"))))\
       .drop("Dedup")

df = df.filter(col('Dedup') == 1)

df.createOrReplaceTempView("srcTemp")

df = spark.sql("""
              SELECT * ,
              current_timestamp() as start_Time,
              CAST('3000-01-01' AS TIMESTAMP) as end_Time,
              "Y" as isActive
              FROM srcTemp
              
              """)

df.createOrReplaceTempView("src")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM src

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `MERGE-1` : `Marking The Updated Records As Expired`

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO pyspark_cata.source.DimCustomers AS trg
# MAGIC USING src AS src
# MAGIC ON trg.id = src.id
# MAGIC AND trg.isActive = 'Y'
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     src.email <> trg.email
# MAGIC     OR src.city <> trg.city
# MAGIC     OR src.country <> trg.country
# MAGIC     OR src.modifiedDate <> trg.modifiedDate
# MAGIC )
# MAGIC
# MAGIC THEN UPDATE SET
# MAGIC trg.end_Time = current_timestamp(),
# MAGIC trg.isActive = 'N'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `Merge-2` : `Upserting The Record`

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO pyspark_cata.source.DimCustomers AS trg
# MAGIC USING src AS src
# MAGIC ON trg.id = src.id
# MAGIC AND trg.isActive = 'Y'
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

