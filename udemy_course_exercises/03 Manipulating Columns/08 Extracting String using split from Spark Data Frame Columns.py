# Databricks notebook source
l = [('X',)]

# COMMAND ----------

df = spark.createDataFrame(l, "Dummy String")

# COMMAND ----------

from pyspark.sql.functions import split, lit, explode

# COMMAND ----------

df.select(split(lit("Hello world how are you"), " ")).show(truncate=False)

# COMMAND ----------

df.select(split(lit("Hello world how are you"), " ")[2]).show(truncate=False)

# COMMAND ----------

df.select(
    explode(split(lit("Hello world how are you"), " ")).\
    alias("word")
).show(truncate=False)

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890,+1 234 567 8901", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111,+44 222 222 2222", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210,+61 876 543 2109", "789 12 6118"
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

employeeDF.\
    select("employee_id", "phone_number").\
    withColumn('phone_number_array', (split('phone_number', ','))).\
    show(truncate = False)

# COMMAND ----------

employeeDF.show()

# COMMAND ----------

employeeDF = employeeDF.\
    select("employee_id", "phone_number").\
    withColumn('phone_number_seperated', explode(split('phone_number', ',')))

# COMMAND ----------

employeeDF.show()

# COMMAND ----------

employeeDF.\
    withColumn('phone_number_area_code', split('phone_number_seperated', ' ')[1]).show()

# COMMAND ----------


