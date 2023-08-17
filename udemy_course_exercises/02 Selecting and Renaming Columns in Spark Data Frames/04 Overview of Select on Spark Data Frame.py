# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------

help(users_df.select)

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()

# COMMAND ----------

users_df.select(['id', 'first_name', 'last_name']).show()

# COMMAND ----------

users_df.alias('u').select('u.*').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select(col('id'),'first_name', 'last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

users_df.select(
    col('id'),
    'first_name',
    'last_name',
    concat(col('first_name'), lit(', '), col('last_name')).alias('full_name')
).show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    select * from users
""").show()
