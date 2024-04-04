# Databricks notebook source

spark.conf.set("fs.azure.account.auth.type.adlslakshika.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlslakshika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlslakshika.dfs.core.windows.net", "5219a421-3fc5-463b-b2a3-c7d2d07a2231")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlslakshika.dfs.core.windows.net", "jhT8Q~QDqurgqarFG4cEZi9uu2MFbQFHz3yXrcpi")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlslakshika.dfs.core.windows.net", "https://login.microsoftonline.com/adb53b4f-b05f-4dcb-a2e1-9111380568c3/oauth2/token")

# COMMAND ----------

source = 'abfss://test@adlslakshika.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Reading data from CSV fie 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, DoubleType

schema1 = StructType([
        StructField('Education_Level', StringType()),
        StructField('Line_Number', IntegerType()),
        StructField('Employed', IntegerType()),        
        StructField('Unemployed', IntegerType()),
        StructField('Industry', StringType()), 
        StructField('Gender', StringType()),
        StructField('Date_Inserted', StringType()),  
        StructField('Dense_Rank', IntegerType()),                     
])

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').schema(schema1).load(f'{source}/*.csv')

# COMMAND ----------

df.write.format('delta').saveAsTable('delta.VersionTable')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM delta.VersionTable

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Inserting records in the table
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC INSERT INTO delta.VersionTable
# MAGIC VALUES 
# MAGIC     ('Bachelor', 1, 4500, 500, 'Networking', 'Male', '2023-07-12', 1),
# MAGIC     ('Master', 2, 6500, 500, 'Networking', 'Female', '2023-07-12', 2),
# MAGIC     ('Hight School', 3, 3500, 500, 'Networking', 'Male', '2023-07-12', 3),
# MAGIC     ('Phd', 4, 5500, 500, 'Networking', 'Female', '2023-07-12', 4)        
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY delta.VersionTable

# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/versiontable')

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE delta.VersionTable
# MAGIC SET education_level = 'PhD'
# MAGIC WHERE Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM delta.VersionTable
# MAGIC WHERE  Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE HISTORY delta.VersionTable

# COMMAND ----------

# MAGIC %md 
# MAGIC  
# MAGIC ###Using versionAsOf - Pyspark

# COMMAND ----------

df_1 = (spark.read.format('delta')
        .option('versionAsOf','1')
        .load('dbfs:/user/hive/warehouse/delta.db/versiontable'))

# COMMAND ----------

display(df_1.filter("Industry == 'Networking'").select("Education_Level", "Industry" ))

# COMMAND ----------

df_2 = (spark.read.format('delta')
        .load('dbfs:/user/hive/warehouse/delta.db/versiontable@v1'))

# COMMAND ----------

display(df_2.filter("Industry == 'Networking'").select("Education_Level", "Industry" ))

# COMMAND ----------

# MAGIC %md 
# MAGIC  
# MAGIC ###Using versionAsOf - SQL

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT education_level, Industry 
# MAGIC FROM delta.VersionTable VERSION AS OF 1
# MAGIC WHERE  Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT education_level, Industry 
# MAGIC FROM delta.`dbfs:/user/hive/warehouse/delta.db/versiontable@v1`
# MAGIC WHERE  Industry = 'Networking'

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Using timestampAsOf

# COMMAND ----------

df_t_1 = (spark.read.format('delta')
            .option('timestampAsOf', '2024-03-27T08:49:08Z')
            .load('dbfs:/user/hive/warehouse/delta.db/versiontable'))

# COMMAND ----------

df_t_1.filter("Industry = 'Networking'").select("Education_Level", "Industry").show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT education_level, Industry 
# MAGIC FROM delta.VersionTable timestamp AS OF '2024-03-27T08:49:08Z'
# MAGIC WHERE  Industry = 'Networking'

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ###Using RESTORE command to get previous version to Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC RESTORE TABLE delta.VersionTable TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT education_level, Industry 
# MAGIC FROM delta.VersionTable 
# MAGIC WHERE  Industry = 'Networking'

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE HISTORY delta.VersionTable 