# Databricks notebook source
print("hello world")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog dev_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --CREATE SCHEMA IF NOT EXISTS `bronze`
# MAGIC   --MANAGED LOCATION 'abfss://transform@adlsgen2ma.dfs.core.windows.net/bronze'

# COMMAND ----------

display(spark.sql(""" DESCRIBE EXTERNAL LOCATION silver """))

# COMMAND ----------

display(spark.sql(""" DESCRIBE EXTERNAL LOCATION silver """).select("url"))

# COMMAND ----------

display(spark.sql(""" DESCRIBE EXTERNAL LOCATION silver """).select("url").collect()[0][0])

# COMMAND ----------

#silver_path = spark.sql(""" DESCRIBE EXTERNAL LOCATION `silver` """).select("url").collect()[0][0]

# COMMAND ----------

#print(silver_path)

# COMMAND ----------

#spark.sql(f""" CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{silver_path}' """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Scripts

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

print(env)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run common script

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajiths0098@outlook.com/04. Common"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Schema

# COMMAND ----------

def create_bronze_schema(environment, path):
    print(f"Using {environment}_catalog")
    spark.sql(f""" USE CATALOG '{environment}_catalog' """)
    print(f"Creating Bronze Schema in {environment}_catalog")
    spark.sql(f""" CREATE SCHEMA IF NOT EXISTS `bronze` MANAGED LOCATION '{path}' """)

# COMMAND ----------

def create_silver_schema(environment, path):
    print(f"Using {environment}_catalog")
    spark.sql(f""" USE CATALOG '{environment}_catalog' """)
    print(f"Creating Silver Schema in {environment}_catalog")
    spark.sql(f""" CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{path}' """)

# COMMAND ----------

def create_gold_schema(environment, path):
    print(f"Using {environment}_catalog")
    spark.sql(f""" USE CATALOG '{environment}_catalog' """)
    print(f"Creating Gold Schema in {environment}_catalog")
    spark.sql(f""" CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{path}' """)

# COMMAND ----------

create_bronze_schema(env, bronze_path)
create_silver_schema(env, silver_path)
create_gold_schema(env, gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating Bronze Tables

# COMMAND ----------

def createTable_rawTraffic(env):
    print(f"Creating raw_traffic in {env}_catalog")
    spark.sql(f""" CREATE TABLE IF NOT EXISTS `{env}_catalog`.`bronze`.`raw_traffic`
                    (
                        Record_ID INT,
                        Count_point_id INT,
                        Direction_of_travel VARCHAR(255),
                        Year INT,
                        Count_date VARCHAR(255),
                        hour INT,
                        Region_id INT,
                        Region_name VARCHAR(255),
                        Local_authority_name VARCHAR(255),
                        Road_name VARCHAR(255),
                        Road_Category_ID INT,
                        Start_junction_road_name VARCHAR(255),
                        End_junction_road_name VARCHAR(255),
                        Latitude DOUBLE,
                        Longitude DOUBLE,
                        Link_length_km DOUBLE,
                        Pedal_cycles INT,
                        Two_wheeled_motor_vehicles INT,
                        Cars_and_taxis INT,
                        Buses_and_coaches INT,
                        LGV_Type INT,
                        HGV_Type INT,
                        EV_Car INT,
                        EV_Bike INT,
                        Extract_Time TIMESTAMP
                    ) 
                """)

# COMMAND ----------

def createTable_rawRoad(env):
    print(f"Creating raw_road in {env}_catalog")
    spark.sql(f""" CREATE TABLE IF NOT EXISTS `{env}_catalog`.`bronze`.`raw_road`
                        (
                            Road_ID INT,
                            Road_Category_Id INT,
                            Road_Category VARCHAR(255),
                            Region_ID INT,
                            Region_Name VARCHAR(255),
                            Total_Link_Length_Km DOUBLE,
                            Total_Link_Length_Miles DOUBLE,
                            All_Motor_Vehicles DOUBLE
                        )
               """)

# COMMAND ----------

createTable_rawTraffic(env)
createTable_rawRoad(env)