# Databricks notebook source
l = [("            Hello      ",)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("Dummy")

# COMMAND ----------

df.show(truncate= False)

# COMMAND ----------

df1 = spark.createDataFrame(l, "Dummy String")

# COMMAND ----------

from pyspark.sql.functions import ltrim, trim, rtrim

# COMMAND ----------

help(ltrim)

# COMMAND ----------

df.withColumn("ltrim", ltrim("Dummy")).\
    withColumn("rtrim", rtrim("Dummy")).\
    withColumn("trim", trim("Dummy")).\
    show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION rtrim').show(truncate=False)

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION ltrim').show(truncate=False)

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION trim').show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

help(expr)

# COMMAND ----------

df.withColumn("Ltrim", expr("trim(LEADING ' ' FROM Dummy)")).\
    withColumn("Rtrim", expr("trim(TRAILING ' ' FROM Dummy)")).\
    withColumn("trim", expr("trim(BOTH ' ' FROM Dummy)")).\
    show()