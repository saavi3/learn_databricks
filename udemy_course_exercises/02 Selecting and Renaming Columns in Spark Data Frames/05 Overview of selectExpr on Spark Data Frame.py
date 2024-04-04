# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame"

# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

users_df.selectExpr('*').show()

# COMMAND ----------

users_df.selectExpr('id', 'first_name','last_name').show()

# COMMAND ----------

users_df.selectExpr(['id', 'first_name', 'last_name']).show()

# COMMAND ----------

users_df.selectExpr('id', 'first_name', 'last_name', "concat(first_name, ', ', last_name) AS full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
    select *
    from users
""")