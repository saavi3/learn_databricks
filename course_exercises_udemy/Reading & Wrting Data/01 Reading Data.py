# Databricks notebook source
spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

type(countries_df)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

countries_df.display()

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.options(header='true').csv("/FileStore/tables/countries.csv")

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv", header= True)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df.describe()

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv", header= True, inferSchema= True)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

countries_schema = StructType([
                    StructField("COUNTRY_ID", IntegerType(), False),
                    StructField("NAME", StringType(), False),
                    StructField("NATIONALITY", StringType(), False),
                    StructField("COUNTRY_CODE", StringType(), False),
                    StructField("ISO_ALPHA2", StringType(), False),
                    StructField("CAPITAL", StringType(), False),
                    StructField("POPULATION", DoubleType(), False),
                    StructField("AREA_KM2", IntegerType(), False),
                    StructField("REGION_ID", IntegerType(), True),
                    StructField("SUB_REGION_ID", IntegerType(), True),
                    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
                    StructField("ORGANIZATION_REGION_ID", IntegerType(), True),
                    ])

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv", header=True, schema=countries_schema)

# COMMAND ----------

countries_df = spark.read.options(header=True).schema(countries_schema).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_sl_json = spark.read.json("/FileStore/tables/countries_single_line.json")

# COMMAND ----------

countries_sl_json.display()

# COMMAND ----------

countries_ml_json = spark.read.json("/FileStore/tables/countries_multi_line.json", multiLine= True)

# COMMAND ----------

display(countries_ml_json)