# Databricks notebook source
employees = [(1, "Scott", "Tiger", 1000.0, 10,
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, None,
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, '',
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 10,
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]

# COMMAND ----------

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

from pyspark.sql.functions import expr
employeesDF.\
    withColumn("bonus", expr("""
                                CASE 
                                    WHEN bonus IS NULL OR
                                         bonus = ''
                                    THEN 0
                                    ELSE bonus
                                END 
                            """)).show()

# COMMAND ----------

from pyspark.sql.functions import when, col, lit


# COMMAND ----------

help(when)

# COMMAND ----------

employeesDF.\
    withColumn(
        "bonus", 
        when((col("bonus").isNull()) | (col("bonus") == lit('')), 0).otherwise(col("bonus"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Create a dataframe using list called as persons and categorize them based up on following rules.
# MAGIC
# MAGIC |Age range|Category|
# MAGIC |---------|--------|
# MAGIC |0 to 2 Months|New Born|
# MAGIC |2+ Months to 12 Months|Infant|
# MAGIC |12+ Months to 48 Months|Toddler|
# MAGIC |48+ Months to 144 Months|Kids|
# MAGIC |144+ Months|Teenager or Adult|

# COMMAND ----------

persons = [
    (1, 1),
    (2, 13),
    (3, 18),
    (4, 60),
    (5, 120),
    (6, 0),
    (7, 12),
    (8, 160)
]

# COMMAND ----------

persondf = spark.createDataFrame(persons, schema='id INT, age INT')

# COMMAND ----------

persondf.\
    withColumn("type", expr("""
                            CASE 
                                WHEN age < 2 THEN 'New Born'
                                WHEN age >= 2 AND age < 12 THEN'Infant'
                                WHEN age >= 12 AND age < 48 THEN 'Toddler'
                                WHEN age >= 48 AND age < 144 THEN 'Kids'
                            ELSE 'Teenager or Adult'
                            END
                            """)).show()

# COMMAND ----------

persondf.\
    withColumn("type", 
               when(col("age")< 2, 'New Born').
               when((col("age")>= 2) & (col("age")<12), 'Infant').
               when((col("age")>= 12) & (col("age")<48), 'Toddler').
               when((col("age")>= 48)& (col("age") < 144), 'Kids').                                             
               otherwise('Teenager or Adult')).show()

# COMMAND ----------

