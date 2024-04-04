-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SQL Syntax

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Using SQL
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA delta

-- COMMAND ----------

CREATE TABLE `delta`.deltafile
(
  Education_Level VARCHAR(50),
  Line_Number INT,
  Employed INT,
  Unemployed INT,
  Industry VARCHAR(50),
  Gender VARCHAR(10),
  Date_Inserted DATE,
  Dense_Rank INT
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile'))
-- MAGIC #dbutils.fs.help()

-- COMMAND ----------

-- DBTITLE 1,Check the log files
-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log')

-- COMMAND ----------

-- DBTITLE 1,See the content in log file

select *
from text.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000000.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.text('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000000.json'))

-- COMMAND ----------

INSERT INTO `delta`.deltafile
VALUES
    ('Bachelor', 1, 4500, 500, 'IT', 'Male', '2023-07-12',  1),
    ('Master', 2, 6500, 500, 'Finance', 'Female', '2023-07-12', 2),
    ('High School', 3, 3500, 500, 'Retail', 'Male', '2023-07-12', 3),
    ('PhD', 4, 5500, 500, 'Healthcare', 'Female', '2023-07-12', 4);



-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.text('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000001.json'))

-- COMMAND ----------

select * 
from delta.deltafile

-- COMMAND ----------

update `delta`.deltafile
set Industry = 'Finance'
where Education_Level = 'PhD'

-- COMMAND ----------

select *
from delta.deltafile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log'))

-- COMMAND ----------

select *
from text.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000002.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile'))

-- COMMAND ----------

DELETE FROM `delta`.deltafile
WHERE Dense_Rank = 2

-- COMMAND ----------

SELECT * 
FROM `delta`.deltafile

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Reading the delta file using python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('delta').load('dbfs:/user/hive/warehouse/delta.db/deltafile'))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC #Using PySpark    

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.auth.type.adlslakshika.dfs.core.windows.net", "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.adlslakshika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.adlslakshika.dfs.core.windows.net", "5219a421-3fc5-463b-b2a3-c7d2d07a2231")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.adlslakshika.dfs.core.windows.net", "jhT8Q~QDqurgqarFG4cEZi9uu2MFbQFHz3yXrcpi")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlslakshika.dfs.core.windows.net", "https://login.microsoftonline.com/adb53b4f-b05f-4dcb-a2e1-9111380568c3/oauth2/token")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC source = 'abfss://test@adlslakshika.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('parquet').load(f'{source}/ParquetFolder/')

-- COMMAND ----------

drop table `delta`.DeltaSpark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format('delta').mode('overwrite').saveAsTable('`delta`.DeltaSpark')

-- COMMAND ----------

