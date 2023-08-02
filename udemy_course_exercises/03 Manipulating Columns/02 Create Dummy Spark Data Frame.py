# Databricks notebook source
l = [('X', )]

# COMMAND ----------

df = spark.createDataFrame(l, "dummy STRING")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

df.select(current_date().alias('current_date')).show()

# COMMAND ----------


