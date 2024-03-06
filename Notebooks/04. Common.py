# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## To re use common functions and variables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Variables

# COMMAND ----------

checkpoint_path = spark.sql(""" DESCRIBE EXTERNAL LOCATION `checkpoints` """).select("url").collect()[0][0]
landing_path = spark.sql(""" DESCRIBE EXTERNAL LOCATION `landing` """).select("url").collect()[0][0]
bronze_path = spark.sql(""" DESCRIBE EXTERNAL LOCATION `bronze` """).select("url").collect()[0][0]
silver_path = spark.sql(""" DESCRIBE EXTERNAL LOCATION `silver` """).select("url").collect()[0][0]
gold_path = spark.sql(""" DESCRIBE EXTERNAL LOCATION `silver` """).select("url").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2) Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01 - Removing Duplicates

# COMMAND ----------

def remove_duplicates(df):
    print("Removing Duplicate values : ", end='')
    df_dup = df.dropDuplicates()
    print("Success !!")
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02 - Handling NULLs

# COMMAND ----------

# replace NULL by unknown if it is a String col
# replace NULL by 0 if it is a Int col

def handle_nulls(df, columns):
    print("Handling NULL values on String column by replacing with 'Unknown' : ", end='')
    df_strng = df.fillna('Unknown', subset = columns)
    print("Success !!")

    print("Handling NULL values on Integer column by replacing with '0' : ", end='')
    df_clean = df_strng.fillna(0, subset = columns)
    print("Success !!")

    return df_clean