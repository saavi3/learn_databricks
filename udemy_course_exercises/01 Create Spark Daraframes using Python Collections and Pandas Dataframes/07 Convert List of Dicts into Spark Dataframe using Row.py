# Databricks notebook source
users_list = [
     {'user_id':1 , 'user_first_name': 'Scott'},
     {'user_id':2 , 'user_first_name': 'Donald'},
     {'user_id':3 , 'user_first_name': 'Mickey'},
     {'user_id':4 , 'user_first_name': 'Elvis'},     
]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

users_details = users_list[1]

# COMMAND ----------

users_details

# COMMAND ----------

users_details.values()

# COMMAND ----------

Row(*users_details.values())

# COMMAND ----------

user_rows =  [Row(*user.values()) for user in users_list]

# COMMAND ----------

user_rows

# COMMAND ----------

spark.createDataFrame(user_rows, 'user_id int, user_first_name string')

# COMMAND ----------

user_rows = [Row(**user) for user in users_list]

# COMMAND ----------

user_rows

# COMMAND ----------

spark.createDataFrame(user_rows)

# COMMAND ----------

def dummy(**kwargs):
    print(kwargs)
    print(len(kwargs))

# COMMAND ----------

users_details = {'user_id' :1 , 'user_first_name': 'Scott'}

# COMMAND ----------

dummy(user_details = users_details)

# COMMAND ----------

dummy(user_id = 1, user_first_name = 'Scott')

# COMMAND ----------

dummy(**users_details)