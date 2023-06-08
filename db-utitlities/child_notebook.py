# Databricks notebook source
print("I am the child notebook")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC #Wideget utilities

# COMMAND ----------

dbutils.widgets.text("input", "", "provide the input value")

# COMMAND ----------

input_value = dbutils.widgets.get("input")

# COMMAND ----------

print(input_value)

# COMMAND ----------

dbutils.notebook.exit(100)
