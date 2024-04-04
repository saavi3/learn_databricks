# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

full_name = concat('first_name', lit(', '), 'last_name').alias('full_name')
users_df.select('id', full_name).show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

customer_from = date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
users_df.select('id', full_name, customer_from).show()