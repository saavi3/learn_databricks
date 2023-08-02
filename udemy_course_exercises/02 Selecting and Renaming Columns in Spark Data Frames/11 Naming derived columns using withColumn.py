# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import concat,lit, col

# COMMAND ----------

users_df.\
    select('id', 'first_name', 'last_name').\
    withColumn('full_name', concat('first_name', lit(', '), 'last_name')).\
    show()


# COMMAND ----------

users_df.\
    select('id', 'first_name', 'last_name').\
    withColumn('full_name', 'first_name').\
    show()

# COMMAND ----------

users_df.\
    select('id', 'first_name', 'last_name').\
    withColumn('full_name', col('first_name')).\
    show()

# COMMAND ----------

users_df.\
    select('id', 'first_name', 'last_name').\
    withColumn('full_name', users_df['first_name']).\
    show()

# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

users_df.select('id', 'courses').show()

# COMMAND ----------

users_df.select('id', 'courses').withColumn('course_count', size('courses')).show()

# COMMAND ----------



# COMMAND ----------


