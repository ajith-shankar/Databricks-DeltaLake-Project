# Databricks notebook source
# MAGIC %md
# MAGIC #### Run common script

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajiths0098@outlook.com/04. Common"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating widgets

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Roads data from Bronze 

# COMMAND ----------

def read_from_bronze(environment):
    print("Reading Bronze table")
    # Read data using spark structured streaming
    df_bronze_roads  = ( spark.readStream
                                .table("`dev_catalog`.`bronze`.`raw_roads`"))
    print(f"reading {environment}_catalog.bronze.raw_roads is successful")

    return df_bronze_roads

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating road_category_name column

# COMMAND ----------

def road_category(df):
    print("Creating road_category_name column .... :  ", end='')
    from pyspark.sql.functions import when, col

    df_road_cat = df.withColumn('Road_Category_Name', when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                                .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                                .when(col('Road_Category') == 'PA', 'Class A Principal Road')
                                .when(col('Road_Category') == 'PM', 'Class A Principal Motorway')
                                .when(col('Road_Category') == 'M', 'Class B Road')
                                .otherwise('NA')
                                )
    
    print("Success !!")
    return df_road_cat

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Road_Type column

# COMMAND ----------

def road_type(df):
    print("Creating road_type column .... :  ", end='')
    from pyspark.sql.functions import when, col

    df_road_type = df.withColumn('Road_Type', when(col('Road_Category_Name').like('%Class A%'), 'Major')
                                .when(col('Road_Category_Name').like('%Class B%'), 'Minor')
                                .otherwise('NA')
                                )
    
    print("Success !!")
    return df_road_type

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Write transfomed data to Silver_Roads table 

# COMMAND ----------

def write_roads_silver(StreamingDF, environment):
    print(f"Writing the {environment}_catalog.silver.silver_roads data : ",end='')

    write_SilverStream_R = (StreamingDF.writeStream
                            .format('delta')
                            .option('checkpointLocation', checkpoint_path+'/SilverRoadsLoad/Checkpt')
                            .outputMode('append')
                            .queryName("SilverRoadsWriteStream")
                            .trigger(availableNow=True)
                            .toTable(f"`{environment}_catalog`.`silver`.`silver_roads`")
                        )
    
    write_SilverStream_R.awaitTermination()
    print("Success !!")

# COMMAND ----------

# read bronze table
df_roads = read_from_bronze(env)

# remove duplicates
df_noDups = remove_duplicates(df_roads)

# handle NULL
allColumns = df_noDups.schema.names
df_clean = handle_nulls(df_noDups, allColumns)

# create road_category_name column
df_roadCat = road_category(df_clean)

# creatig Road_Type column
df_roadType = road_type(df_roadCat)

# Write to the silver_roads table
write_roads_silver(df_roadType, env)
