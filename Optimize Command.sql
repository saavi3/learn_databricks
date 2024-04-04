-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC  - Deleting records in delta lake -  parquet file state will be set as inactive
-- MAGIC  - Updating records in delta lake -  parquet file state will be set as inactive and new parquet file will be created
-- MAGIC  - Inactive files will be used for time travel 
-- MAGIC  - Optimize command helps to combine all small files into a single large file 1 GB of size

-- COMMAND ----------

CREATE TABLE `delta`.OptimizeTable
(
    Education_Level VARCHAR(50),
    Line_Number INT,
    Employed INT,
    Unemployed INT,
    Industry VARCHAR(50),
    Gender VARCHAR(10),
    Date_Inserted DATE,
    dense_rank INT
)


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### List the location of Delta table  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000000.json'))

-- COMMAND ----------

INSERT INTO delta.OptimizeTable
VALUES
    ('Bachelor', 100,  4500, 500, 'Networking', 'Male', '2023-07-12', 1)


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ##

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000001.json'))
-- MAGIC
-- MAGIC #impacted parquet file - part-00000-a0d1fb4d-8bde-4854-8b12-7ce13fb29ddd.c000.snappy.parquet

-- COMMAND ----------


INSERT INTO delta.OptimizeTable
VALUES
    ('Bachelor', 101, 5200, 700, 'Networking', 'Male', '2023-07-12', 2)


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000002.json'))
-- MAGIC #impacted parquet file - part-00000-3912219f-d616-4180-bc3e-faf23fbe60f2.c000.snappy.parquet

-- COMMAND ----------


INSERT INTO delta.OptimizeTable
VALUES
('Master', 102, 6500, 500, 'Networking', 'Female', '2023-07-12', 3)


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000003.json'))
-- MAGIC #impacted parquet file - part-00000-94ecf672-e248-48a7-b569-1872af5d3ee2.c000.snappy.parquet

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### Listing all the parquet files 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/')

-- COMMAND ----------

DELETE FROM delta.OptimizeTable
WHERE Line_number = 101

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000004.json'))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ##Update a record

-- COMMAND ----------

UPDATE `delta`.OptimizeTable
SET Line_Number = 99
WHERE Line_Number = 102


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000005.json'))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ##Optimize Command
-- MAGIC
-- MAGIC
-- MAGIC ##### ignore inactive records , only active records are combined into a single parquet

-- COMMAND ----------

OPTIMIZE `delta`.optimizetable

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format('text').load('dbfs:/user/hive/warehouse/delta.db/optimizetable/_delta_log/00000000000000000006.json'))

-- COMMAND ----------

-- DBTITLE 1,Time Travel
describe history `delta`.OptimizeTable