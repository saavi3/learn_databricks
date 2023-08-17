# Databricks notebook source
from pyspark.sql.functions import trunc, date_trunc

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

df = spark.createDataFrame(datetimes, schema= "date string, time string")

# COMMAND ----------

df.show()

# COMMAND ----------

help(trunc)

# COMMAND ----------

df.\
    withColumn("date_trunc1", trunc("date", "week")).\
    withColumn("date_trunc2", trunc("date", "quarter")).\
    withColumn("date_trunc3", trunc("date", "month")).\
    withColumn("time_trunc3", trunc("time", "yyyy")).\
        show()

# COMMAND ----------

help(date_trunc)

# COMMAND ----------

df.\
    withColumn("date_trunc1", date_trunc("mm", "date" )).\
    withColumn("date_trunc3", date_trunc("month", "time")).\
    withColumn("time_trunc1", date_trunc("day", "time" )).\
    withColumn("time_trunc2", date_trunc("minute", "time")).\
    withColumn("time_trunc3", date_trunc("hour", "time")).\
        show()

# COMMAND ----------

df.withColumn("time_trunc_minute", date_trunc("minute", "time")).\
    withColumn("time_trunc_hour", date_trunc("hour", "time")).\
    withColumn("time_trunc_day", date_trunc("day", "time" )).\
        show(truncate = False)

# COMMAND ----------

from pyspark.sql.functions import to_date, col

# COMMAND ----------

df.withColumn("to_date", to_date("date")).show()

# COMMAND ----------

df.withColumn("to_date", to_date(col("date"))).show()

# COMMAND ----------


