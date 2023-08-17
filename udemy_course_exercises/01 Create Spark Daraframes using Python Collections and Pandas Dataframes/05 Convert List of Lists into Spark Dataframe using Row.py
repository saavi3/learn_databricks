# Databricks notebook source
user_list = [[1,'Scott'], [2,'Donald'], [3,'Mickey'], [4, 'Elvis']]

# COMMAND ----------

type(user_list)

# COMMAND ----------

type(user_list[2])

# COMMAND ----------

spark.createDataFrame(user_list)

# COMMAND ----------

spark.createDataFrame(user_list, 'user_id int, user_first_name string')

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

users_rows = [Row(*user) for user in user_list]

# COMMAND ----------

spark.createDataFrame(users_rows)

# COMMAND ----------

spark.createDataFrame(users_rows, 'user_id int, user_first_name string')

# COMMAND ----------

def dummy(*args):
    print(args)
    print(len(args))


# COMMAND ----------

dummy(1)

# COMMAND ----------

dummy(1,'Hello')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC '*' enables to take items as seperate arguments
# MAGIC

# COMMAND ----------

user_details = [1, 'Scott']
dummy(user_details)

# COMMAND ----------

dummy(*user_details)

# COMMAND ----------

help(Row)
