# Databricks notebook source
# MAGIC %md
# MAGIC ## `PYTHON CLASS`

# COMMAND ----------

# MAGIC %md
# MAGIC ###### `The Scenario In Which We have to build the Code that We Can Use In Different Projects Without Copy Or Duplicating it`

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

class DataValidation:

    def __init__(self, df):
        self.df = df

    def dedup(self, keycol, cdcCol):

        df = self.df.withColumn("Dedup", row_number().over(Window.partitionBy(keycol).orderBy(desc(cdcCol))))
        df = df.filter(col("Dedup") == 1).drop("Dedup")
        
        return df
    
    def removenulls(self, nullCol):
        df = self.df.filter(col(nullCol).isNotNull())

        return df

# COMMAND ----------

df = spark.createDataFrame([("1", "2020-01-01", 100), ("2", "2020-01-02", 200), ("3", "2020-01-03", 300), ("1", "2020-01-11", 500)], ["order_id", "order_date", "amount"])

display(df)


# COMMAND ----------

cls_obj = DataValidation(df)

# COMMAND ----------

df_dedup = cls_obj.dedup("order_id", "order_date")

# COMMAND ----------

display(df_dedup)

# COMMAND ----------

