# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------

from pyspark.sql.functions import lit, col

# COMMAND ----------

help(lit)

# COMMAND ----------

lit(25)

# COMMAND ----------

lit('25')

# COMMAND ----------

users_df.select('id', 'amount_paid', 'amount_paid'+ lit(25), col('amount_paid')+ lit(25), lit(25.0) + lit(10)).show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')
spark.sql("""
    select id , amount_paid + 25 AS amount_paid
    from users         
""").\
    show()

# COMMAND ----------

users_df.selectExpr('id', 'amount_paid  +25').show()

# COMMAND ----------


