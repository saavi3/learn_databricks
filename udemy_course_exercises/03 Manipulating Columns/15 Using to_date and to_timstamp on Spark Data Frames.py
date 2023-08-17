# Databricks notebook source
from pyspark.sql.functions import to_date, to_timestamp, lit

# COMMAND ----------

l = [('X' ,)]

# COMMAND ----------

df = spark.createDataFrame(l, 'dummy string')

# COMMAND ----------

df.select(to_date(lit('02/03/2021'), 'dd/MM/yyyy')).show()

# COMMAND ----------

df.select(to_date(lit('02-Mar-2021'), 'dd-MMM-yyyy')).show()

# COMMAND ----------


