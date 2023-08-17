# Databricks notebook source
l = [("X", )]

# COMMAND ----------

df = spark.createDataFrame(l, schema="dummy STRING")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date, year, month, weekofyear, dayofmonth, dayofyear,dayofweek

# COMMAND ----------

df.select(current_date().alias('current_date'),
          year(current_date()).alias('year'),
          month(current_date()).alias('month'),
          weekofyear(current_date()).alias('weekofyear'),
          dayofyear(current_date()).alias('dayofyear'),
          dayofmonth(current_date()).alias('dayofmonth'),
          dayofweek(current_date()).alias('dayofweek')).\
    show()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df.select(F.current_timestamp().alias('current_date'),
          F.hour(F.current_timestamp()).alias('hour'),
          F.minute(F.current_timestamp()).alias('minute'),
          F.second(F.current_timestamp()).alias('second')).\
    show(truncate = False)

# COMMAND ----------


