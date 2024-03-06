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
# MAGIC ## Read Silver tables

# COMMAND ----------

def read_from_silver_traffic(environment):
    print("Reading Silver Traffic table")
    # Read data using spark structured streaming
    df_silver_traffic  = ( spark.readStream
                                .table("`dev_catalog`.`silver`.`silver_traffic`"))
    print(f"reading {environment}_catalog.silver.silver_trafffic is successful")

    return df_silver_traffic

# COMMAND ----------

def read_from_silver_road(environment):
    print("Reading Silver Roads table")
    # Read data using spark structured streaming
    df_silver_roads  = ( spark.readStream
                                .table("`dev_catalog`.`silver`.`silver_roads`"))
    print(f"reading {environment}_catalog.silver.silver_roads is successful")

    return df_silver_roads

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Vehicles Intensity column

# COMMAND ----------

def vehicle_intesity(df):
    print("Creating Vehicle_Intesity column ....")
    from pyspark.sql.functions import col

    df_veh_int = df.withColumn('Vehicle_Intensity', col('Motor_Vehicle_Count')/col('Link_Length_km'))

    print("Success !!")
    return df_veh_int

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating LoadTime column

# COMMAND ----------

def laod_time(df):
  print("Creating Load_Time column ....")
  from pyspark.sql.functions import current_timestamp

  df_time_stamp = df.withColumn('Load_Time', current_timestamp())

  print("Success !!")
  return df_time_stamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to gold tables

# COMMAND ----------

def write_gold_traffic(StreamingDF, environment):
    print(f"Writing the {environment}_catalog.gold.gold_traffic data : ",end='')

    write_GoldStream = (StreamingDF.writeStream
                            .format('delta')
                            .option('checkpointLocation', checkpoint_path+'/GoldTrafficLoad/Checkpt')
                            .outputMode('append')
                            .queryName("GoldTrafficWriteStream")
                            .trigger(availableNow=True)
                            .toTable(f"`{environment}_catalog`.`gold`.`gold_traffic`")
                        )
    
    write_GoldStream.awaitTermination()
    print("Success !!")

# COMMAND ----------

def write_gold_roads(StreamingDF, environment):
    print(f"Writing the {environment}_catalog.gold.gold_roads data : ",end='')

    write_GoldStream_R = (StreamingDF.writeStream
                            .format('delta')
                            .option('checkpointLocation', checkpoint_path+'/GoldRoadsLoad/Checkpt')
                            .outputMode('append')
                            .queryName("GoldRoadsWriteStream")
                            .trigger(availableNow=True)
                            .toTable(f"`{environment}_catalog`.`gold`.`gold_roads`")
                        )
    
    write_GoldStream_R.awaitTermination()
    print("Success !!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling all the functions

# COMMAND ----------

# Read silver tables
df_silverTraffic = read_from_silver_traffic(env)
df_silverRoad = read_from_silver_road(env)

# Gold transformation
df_vehicle = vehicle_intesity(df_silverTraffic)
df_goldTraffic = laod_time(df_vehicle)
df_goldRoad = laod_time(df_silverRoad)

# Write to Gold Table
write_gold_traffic(df_goldTraffic, env)
write_gold_roads(df_goldRoad, env)