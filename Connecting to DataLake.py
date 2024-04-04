# Databricks notebook source

spark.conf.set("fs.azure.account.auth.type.adlslakshika.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlslakshika.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlslakshika.dfs.core.windows.net", "5219a421-3fc5-463b-b2a3-c7d2d07a2231")
spark.conf.set("fs.azure.account.oauth2.client.secret.adlslakshika.dfs.core.windows.net", "jhT8Q~QDqurgqarFG4cEZi9uu2MFbQFHz3yXrcpi")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlslakshika.dfs.core.windows.net", "https://login.microsoftonline.com/adb53b4f-b05f-4dcb-a2e1-9111380568c3/oauth2/token")

# COMMAND ----------

df = (spark.read.format('csv')
        .option('header', 'true')
        .load("abfss://test@adlslakshika.dfs.core.windows.net/sample/*.csv"))

# COMMAND ----------

