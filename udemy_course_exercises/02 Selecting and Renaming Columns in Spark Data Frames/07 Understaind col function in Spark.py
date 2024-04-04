# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select(col('id'))

# COMMAND ----------

user_id = col('id')


# COMMAND ----------

users_df.select(user_id).show()

# COMMAND ----------

users_df.dtypes

# COMMAND ----------

from pyspark.sql.functions import date_format, cast

# COMMAND ----------

help(date_format)

# COMMAND ----------

users_df.select(
    'id', date_format('customer_from', 'yyyyMMdd')
).show()

# COMMAND ----------

users_df.select(
    'id', date_format('customer_from', 'yyyyMMdd').alias('customer_from').cast('int')
).show()

# COMMAND ----------

help(cast)

# COMMAND ----------

cols = [col('id'), date_format('customer_from', 'yyyyMMdd').alias('customer_from').cast('int')]
users_df.select(*cols).show()