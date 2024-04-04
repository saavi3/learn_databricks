# Databricks notebook source
datetimes = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                     (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                     (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                     (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

datetimesDF = spark.createDataFrame(datetimes).toDF("dateid", "date", "time")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col

# COMMAND ----------

help(unix_timestamp)

# COMMAND ----------

datetimesDF.\
    withColumn("current_unix_date_id", unix_timestamp()).\
    withColumn("unix_date_id", unix_timestamp(col("dateid").cast('string'), 'yyyyMMdd')).\
    withColumn("unix_date", unix_timestamp("date", 'yyyy-MM-dd')).\
    show()

# COMMAND ----------

unixtimes = [(1393561800, ),
             (1456713488, ),
             (1514701799, ),
             (1567189800, )
            ]

# COMMAND ----------

unixtimesDF = spark.createDataFrame(unixtimes).toDF("unixtime")

# COMMAND ----------

from pyspark.sql.functions import from_unixtime,col

# COMMAND ----------

unixtimesDF.\
    withColumn("date", from_unixtime("unixtime", "yyyyMMdd")).\
    withColumn("time", from_unixtime("unixtime")).\
    show()

# COMMAND ----------

unixtimesDF.\
    select(col('unixtime').cast('date')).show()

# COMMAND ----------

unixtimesDF.\
    select(col('unixtime').cast('timestamp')).show()

# COMMAND ----------

