# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType 

schema = StructType([
                    StructField('Country', StringType()),
                    StructField('Citizens', IntegerType())
])

# COMMAND ----------

source_dir = 'dbfs:/FileStore/FileStore/streaming/'

# COMMAND ----------

df = (spark.readStream.format('csv')
        .option('header', 'true')
        .schema(schema)
        .load(source_dir))

# COMMAND ----------

display(df)


# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE SCHEMA if NOT EXISTS stream;
# MAGIC use stream

# COMMAND ----------

(df.writeStream 
    .option('checkPointLocation', f'{source_dir}/AppendCheckpoint')
    .outputMode("append")
    .queryName('AppendQuery')
    .toTable("stream.AppendTable"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM stream.AppendTable
# MAGIC

# COMMAND ----------

