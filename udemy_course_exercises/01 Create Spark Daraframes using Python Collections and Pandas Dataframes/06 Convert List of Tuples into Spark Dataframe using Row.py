# Databricks notebook source
user_list = [(1,'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]

# COMMAND ----------

type(user_list)

# COMMAND ----------

type(user_list[2])

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

user_rows =  [Row(*user) for user in user_list]

# COMMAND ----------

spark.createDataFrame(user_rows)

# COMMAND ----------

user_rows

# COMMAND ----------

spark.createDataFrame(user_rows)

# COMMAND ----------

spark.createDataFrame(user_rows, 'user_id int, user_name string')

# COMMAND ----------


