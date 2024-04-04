# Databricks notebook source
# MAGIC %md
# MAGIC ##Reading overwritten parquet

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.adlslakshika.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlslakshika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlslakshika.dfs.core.windows.net", "5219a421-3fc5-463b-b2a3-c7d2d07a2231")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlslakshika.dfs.core.windows.net", "jhT8Q~QDqurgqarFG4cEZi9uu2MFbQFHz3yXrcpi")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlslakshika.dfs.core.windows.net", "https://login.microsoftonline.com/adb53b4f-b05f-4dcb-a2e1-9111380568c3/oauth2/token")

# COMMAND ----------

source = 'abfss://test@adlslakshika.dfs.core.windows.net/'

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

df = spark.read.option('header', 'true').format('csv').schema(schema1).load(f'{source}/*.csv')

# COMMAND ----------

df.write.mode('overwrite').format('parquet').save(f'{source}/OnlyParquet')

# COMMAND ----------

dfNew = df.withColumnRenamed(existing='Line_Number', new='LNo')

# COMMAND ----------

dfNew.write.mode('overwrite').format('parquet').save(f'{source}/OnlyParquet')

# COMMAND ----------

display(spark.read.format('parquet').load(f'{source}/OnlyParquet'))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##Convert to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CONVERT TO DELTA parquet.`abfss://test@adlslakshika.dfs.core.windows.net/OnlyParquet`

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE HISTORY delta.`abfss://test@adlslakshika.dfs.core.windows.net/OnlyParquet`

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC after converting to delta the changes made for parquet (column rename) is not captured coz it was done before converting to delta