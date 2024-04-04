# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #Schema Enforce 

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.adlslakshika.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlslakshika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlslakshika.dfs.core.windows.net", "5219a421-3fc5-463b-b2a3-c7d2d07a2231")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlslakshika.dfs.core.windows.net", "jhT8Q~QDqurgqarFG4cEZi9uu2MFbQFHz3yXrcpi")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlslakshika.dfs.core.windows.net", "https://login.microsoftonline.com/adb53b4f-b05f-4dcb-a2e1-9111380568c3/oauth2/token")

# COMMAND ----------

source = 'abfss://test@adlslakshika.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #Reading data with More Columns 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

schema1 = StructType([
        StructField('Education_Level', StringType()),
        StructField('Line_Number', IntegerType()),
        StructField('Employed', IntegerType()),        
        StructField('Unemployed', IntegerType()),
        StructField('Industry', StringType()), 
        StructField('Gender', StringType()),
        StructField('Date_Inserted', StringType()),  
        StructField('Dense_Rank', IntegerType()),  
        StructField('Max_Salary_USD', IntegerType()),                     
])

# COMMAND ----------

df_moreCols = (spark.read.format('csv')
                    .schema(schema1)
                    .option('header', 'true')
                    .load(f'{source}/ShcemaEvol/SchemaMoreCols.csv'))

# COMMAND ----------

df_moreCols.printSchema()

# COMMAND ----------

display(df_moreCols)

# COMMAND ----------

df_moreCols.write.format('delta').mode('append').saveAsTable('`delta`.deltaSpark')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #Reading data with Less Columns 

# COMMAND ----------

from  pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, BooleanType

schema2 = StructType([
        StructField('Education_Level', StringType()),
        StructField('Line_Number', IntegerType()),
        StructField('Employed', IntegerType()),        
        StructField('Unemployed', IntegerType()),
        StructField('Industry', StringType()), 
        StructField('Gender', StringType())                    
])

# COMMAND ----------

df_lessCols = spark.read.format('csv').schema(schema2).option('hearder', 'true').load(f'{source}/ShcemaEvol/SchemaMoreCols.csv')

# COMMAND ----------

df_lessCols.printSchema()

# COMMAND ----------

df_lessCols.write.format('delta').mode('append').saveAsTable('`delta`.deltaSpark')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.deltaSpark

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Source data with different data type

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').load(f'{source}/*.csv')

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable('`delta`.deltaSpark')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #Schema Evolution 
# MAGIC
# MAGIC ##### Allow changes for extra cols 
# MAGIC

# COMMAND ----------

df_moreCols.write.format('delta').mode('append').option('mergeSchema', 'True').saveAsTable('`delta`.deltaSpark')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM delta.deltaSpark

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ###### Allow different schema to evolve 
# MAGIC
# MAGIC Not recommended in real scenarios 

# COMMAND ----------

df.write.format('delta').mode('overwrite').option('overwriteSchema', 'True').saveAsTable('`delta`.deltaSpark')

# COMMAND ----------

