# Databricks notebook source
print("I am the child notebook")

# COMMAND ----------

# MAGIC %md 
# MAGIC #Wideget utilities

# COMMAND ----------

dbutils.widgets.text("input1", "", "provide the input value 1")
dbutils.widgets.text("input2", "", "provide the input value 2")

# COMMAND ----------

input_value1 = int(dbutils.widgets.get('input1'))
input_value2 = int(dbutils.widgets.get('input2'))


# COMMAND ----------

output_value =  input_value1 + input_value2
print(output_value)

# COMMAND ----------

dbutils.notebook.exit(f"Notebook executed sucessfully and returned {output_value}")

# COMMAND ----------

print("Test text to see if this runs after the exit command")