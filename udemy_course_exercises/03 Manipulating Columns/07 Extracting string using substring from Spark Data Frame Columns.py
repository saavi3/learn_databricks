# Databricks notebook source
s = "Hello world"

# COMMAND ----------

s[:5]

# COMMAND ----------

s[1:4]

# COMMAND ----------

l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l, "Dummy STRING")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import substring,lit, col

# COMMAND ----------

df.select(substring(lit("Hello world"), 6, 7)).show()

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, 
      "united states", "+1 123 456 7890", "123 45 6789"
    ),
     (2, "Henry", "Ford", 1250.0, 
      "India", "+91 234 567 8901", "456 78 9123"
     ),
     (3, "Nick", "Junior", 750.0, 
      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
     ),
     (4, "Bill", "Gomes", 1500.0, 
      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
     )
]

# COMMAND ----------

employeeDF = spark.\
    createDataFrame(employees,
                    schema = """employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING""")

# COMMAND ----------

employeeDF.show()

# COMMAND ----------

employeeDF.withColumn("last_ssn", substring("ssn", 8, 4)).show()

# COMMAND ----------

employeeDF.withColumn("phone_last4", substring("phone_number", -4, 4)).show()

# COMMAND ----------

4