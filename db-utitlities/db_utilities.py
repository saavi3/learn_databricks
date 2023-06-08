# Databricks notebook source
# MAGIC %md 
# MAGIC #Filesystem utilities

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

ls 'dbfs:/databricks-datasets/'

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

dbutils.notebook.run('./child_notebook', 10)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Wideget utilities

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.notebook.run("./child_notebook", 10, {"input" : "This is the input value sent by parent notebook"})

# COMMAND ----------

# MAGIC %md 
# MAGIC #Library Utilities
# MAGIC

# COMMAND ----------

# MAGIC %pip install pandas
