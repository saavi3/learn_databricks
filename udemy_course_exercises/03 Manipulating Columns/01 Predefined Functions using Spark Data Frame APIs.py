# Databricks notebook source
# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

orders = spark.read.csv(
    '/public/retail_db/orders',
    schema=' order_id INT, order_date STRING, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format
orders.select('*', date_format('order_date', 'yyyyMM').alias('order_month')).show()

# COMMAND ----------

orders.withColumn('order_month',date_format('order_date', 'yyyyMM')).show()

# COMMAND ----------

orders.\
    filter(date_format('order_date', 'yyyyMM')== 201401).\
    show()

# COMMAND ----------

orders.\
    groupBy(date_format('order_date', 'yyyyMM')).\
    count().\
    show()

