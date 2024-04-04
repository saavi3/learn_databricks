# Databricks notebook source
# MAGIC %md 
# MAGIC #Filesystem utilities

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables')

# COMMAND ----------

ls 'dbfs:/FileStore/tables'

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for foldername in dbutils.fs.ls('/'):
    print(foldername)

# COMMAND ----------

#all the files system commands avaialbale 
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('mount')

# COMMAND ----------

# MAGIC %md 
# MAGIC #Notebook utilities
# MAGIC

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run('./db_utilities_child_notebook', 60, {'input1' :400, 'input2' :120})

# COMMAND ----------

# MAGIC %md 
# MAGIC #Wideget utilities

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Combo Box
# MAGIC

# COMMAND ----------

dbutils.widgets.combobox(name= 'combox_name', defaultValue='Employee', choices=['Employee', 'Developer', 'Tester', 'Manager'], label= 'Department Label')

# COMMAND ----------

combox_name = dbutils.widgets.get('combox_name')
print(combox_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop down

# COMMAND ----------

dbutils.widgets.dropdown(name= 'dropdown_name', defaultValue='Employee', choices=['Employee', 'Developer', 'Tester', 'Manager'], label= 'Department Label')

# COMMAND ----------

dbutils.widgets.get('dropdown_name')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Multi Select

# COMMAND ----------

dbutils.widgets.multiselect(name= 'multiselect_name', defaultValue='Employee', choices=['Employee', 'Developer', 'Tester', 'Manager'], label= 'Department Label')

# COMMAND ----------

dbutils.widgets.get('multiselect_name')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Text

# COMMAND ----------

dbutils.widgets.text(name='text_name', defaultValue='', label='Text Label')

# COMMAND ----------

dbutils.widgets.get('text_name')

# COMMAND ----------

result = dbutils.widgets.get('text_name')

print(f"SELECT * FROM Schema.Table WHERE Year = {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC On the wideget panel setting you can select the action to be taken once a widget value changed 
# MAGIC
# MAGIC - Run Notebook
# MAGIC - Run Accessed Command
# MAGIC - Do nothing
# MAGIC

# COMMAND ----------

dbutils.notebook.run("./child_notebook", 10, {"input" : "This is the input value sent by parent notebook"})

# COMMAND ----------

# MAGIC %md 
# MAGIC #Library Utilities
# MAGIC

# COMMAND ----------

# MAGIC %pip install pandas