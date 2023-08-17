# Databricks notebook source
l = [('X', )]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("Dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp

# COMMAND ----------

df.select(current_date()).show()

# COMMAND ----------

df.select(current_timestamp()).show(truncate = False)

# COMMAND ----------

from pyspark.sql.functions import lit, to_date, to_timestamp

# COMMAND ----------

df.select(to_date(lit('20210128'),'yyyyMMdd')).show()

# COMMAND ----------

df.select(to_timestamp(lit('20210128 1225'),'yyyyMMdd HHmm')).show()

# COMMAND ----------


