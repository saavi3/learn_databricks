# Databricks notebook source
# MAGIC %md 
# MAGIC #Reading Files

# COMMAND ----------

# DBTITLE 1,Azure Data Lake Connectivity Setup

spark.conf.set("fs.azure.account.auth.type.adlslakshika.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlslakshika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlslakshika.dfs.core.windows.net", "5219a421-3fc5-463b-b2a3-c7d2d07a2231")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlslakshika.dfs.core.windows.net", "jhT8Q~QDqurgqarFG4cEZi9uu2MFbQFHz3yXrcpi")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlslakshika.dfs.core.windows.net", "https://login.microsoftonline.com/adb53b4f-b05f-4dcb-a2e1-9111380568c3/oauth2/token")

# COMMAND ----------

source = 'abfss://test@adlslakshika.dfs.core.windows.net/'

# COMMAND ----------

# DBTITLE 1,Schema Creation
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

# DBTITLE 1,Load the CSV file to a dataframe
df = (spark.read.format('csv')
    .option('header', 'true', )
    .schema(schema1)
    .load(f'{source}/*.csv'))

# COMMAND ----------

# DBTITLE 1,Print Schema
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #Writing Files 

# COMMAND ----------

# DBTITLE 1,Write the dataframe as a parquet file
(df.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/ParquetFolder/'))

# COMMAND ----------

# DBTITLE 1,Load the parquet to a dataframe
df_parquet = spark.read.format('parquet').load(f'{source}/ParquetFolder/')

# COMMAND ----------

df_parquet.printSchema()

# COMMAND ----------

display(df_parquet)

# COMMAND ----------

# DBTITLE 1,Filter data from the dataframe
df_parquet = df_parquet.filter("Education_Level = 'High School'")

# COMMAND ----------

# DBTITLE 1,Save fitered data into datalake
df_parquet.write.format('parquet').mode('overwrite').save(f'{source}/ParquetFolder/temp')

# COMMAND ----------

display(spark.read.format('parquet').load(f'{source}/ParquetFolder/temp'))

# COMMAND ----------

# MAGIC %md 
# MAGIC #Creating a delta lake  
# MAGIC

# COMMAND ----------

df.write.format('delta').mode('overwrite').save(f'{source}/delta/')

# COMMAND ----------

