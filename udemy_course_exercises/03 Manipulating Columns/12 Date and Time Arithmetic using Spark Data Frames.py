# Databricks notebook source
datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

df = spark.createDataFrame(datetimes, schema = "date STRING, time STRING")

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import date_add, date_sub, to_date, to_timestamp

# COMMAND ----------

help(date_add)

# COMMAND ----------

help(date_sub)

# COMMAND ----------

df.withColumn("dates_added_date", date_add("date", 10)).\
    withColumn("dates_added_time", date_add("time", 10)).\
    withColumn("dates_sub_date", date_add("date", 10)).\
    withColumn("dates_sub_time", date_add("time", 10)).\
    show()

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp, datediff

# COMMAND ----------

help(datediff)

# COMMAND ----------

df.withColumn("datediff_date" , datediff(current_date(), "date")).\
   withColumn("datediff_time" , datediff(current_timestamp(), "time")).\
    show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import months_between, add_months, round

# COMMAND ----------

help(months_between)

# COMMAND ----------

help(add_months)

# COMMAND ----------

df.withColumn("months_between_date_without_round", months_between(current_date(),"date", False)).\
    withColumn("months_between_date", months_between(current_date(),"date")).\
    withColumn("months_between_date_with_round", round(months_between(current_date(),"date"),2)).show()

# COMMAND ----------

df.withColumn("months_between_date_with_round", round(months_between(current_date(),"date"),2)).\
    withColumn("months_between_time", round(months_between(current_timestamp(),"time"),2)).\
    withColumn("add_months_date", add_months("date",3)).\
    withColumn("add_months_time", add_months("time",3)).\
        show()
    

# COMMAND ----------

