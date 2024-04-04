# Databricks notebook source
datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

df = spark.createDataFrame(datetimes, schema= "date string, time string")

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

df.\
    withColumn("time_ts", date_format('time', 'yyyyMMddHHmmss')).\
    withColumn("date_1", date_format('date', 'dd-MMM-yyyy')).\
    withColumn("date_2", date_format('time', 'yyyyDDD')).\
    show()

# COMMAND ----------

df.\
    withColumn("time_ts", date_format('date', 'MMM d, yyyy')).\
    withColumn("date_1", date_format('date', 'EE')).\
    withColumn("date_2", date_format('time', 'EEEE')).\
    show()

# COMMAND ----------

