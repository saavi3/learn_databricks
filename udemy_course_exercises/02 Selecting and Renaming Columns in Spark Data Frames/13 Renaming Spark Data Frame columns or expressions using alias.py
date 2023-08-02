# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

users_df.\
    select(
        col('id').alias('user_id'),
        col('first_name').alias('user_first_name'),
        col('last_name').alias('user_last_name'),
        concat(col('first_name'), lit(', '), col('last_name')).alias('user_full_name')
    ).\
    show()

# COMMAND ----------

users_df.\
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        concat(users_df['first_name'], lit(', '), users_df['last_name']).alias('user_full_name')
    ).\
    show()

# COMMAND ----------

users_df.\
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name')
    ).\
    withColumn('user_full_name', concat(col('user_first_name'), lit(', '), col('user_last_name'))).\
    show()

# COMMAND ----------

users_df.\
    withColumn('user_full_name', concat(col('first_name'), lit(', '), col('last_name'))).\
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        'user_last_name'
    ).\
      show()

# COMMAND ----------


