# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC Vaccum command is capable of deleting parquet files if 

# COMMAND ----------

# MAGIC %md
# MAGIC You can remove data files no longer referenced by a Delta table that are older than the retention threshold by running the VACUUM command on the table. Running VACUUM regularly is important for cost and compliance because of the following considerations:
# MAGIC
# MAGIC Deleting unused data files reduces cloud storage costs.
# MAGIC
# MAGIC Data files removed by VACUUM might contain records that have been modified or deleted. Permanently removing these files from cloud storage ensures these records are no longer accessible.
# MAGIC
# MAGIC The default retention threshold for data files after running VACUUM is 7 days. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC VACUUM delta.deltafile

# COMMAND ----------

# MAGIC %md 
# MAGIC VACUUM eventsTable   -- vacuum files not required by versions older than the default retention period
# MAGIC
# MAGIC VACUUM '/data/events' -- vacuum files in path-based table
# MAGIC
# MAGIC VACUUM delta.`/data/events/`
# MAGIC
# MAGIC VACUUM delta.`/data/events/` RETAIN 100 HOURS  -- vacuum files not required by versions more than 100 hours old
# MAGIC
# MAGIC VACUUM eventsTable DRY RUN    -- do dry run to get the list of files to be deleted
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC VACUUM delta.deltaspark DRY RUN 

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable')