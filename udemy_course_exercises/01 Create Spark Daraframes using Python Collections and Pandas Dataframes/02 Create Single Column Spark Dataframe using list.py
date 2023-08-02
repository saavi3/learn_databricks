# Databricks notebook source
ages_list = [21,23,18,42,41,32]

# COMMAND ----------



# COMMAND ----------

type(ages_list)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(ages_list, int)

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

spark.createDataFrame(ages_list, IntegerType())

# COMMAND ----------

name_list = ['Scot', 'Donal', 'Mickey']

# COMMAND ----------

spark.createDataFrame(name_list,'string')

# COMMAND ----------

from pyspark.sql.types import StringType

# COMMAND ----------

spark.createDataFrame(name_list, StringType())

# COMMAND ----------


