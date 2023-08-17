# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------

# required columns from original list
required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

# new column name list
target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

# COMMAND ----------

help(users_df.toDF)

# COMMAND ----------

users_df.select(required_columns).show()

# COMMAND ----------

# required columns from original list
required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

# new column name list
target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

users_df.\
    select(required_columns).\
    toDF(*target_column_names).\
    show()
