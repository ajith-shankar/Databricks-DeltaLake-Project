# Databricks notebook source
# MAGIC %run "/Workspace/Users/ajiths0098@outlook.com/04. Common"

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating a read_traffic_data() Function

# COMMAND ----------

def read_traffic_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading Raw Traffic data..... ")
    schema = StructType([   StructField("Record_Id", IntegerType()),
                            StructField("Count_point_id", IntegerType()),
                            StructField("Direction_of_travel", StringType()),
                            StructField("Year", IntegerType()),
                            StructField("Count_date", StringType()), 
                            StructField("hour", IntegerType()),
                            StructField("Region_id", IntegerType()),
                            StructField("Region_name", StringType()),
                            StructField("Local_authority_name", StringType()),
                            StructField("Road_name", StringType()),
                            StructField("Road_Category_ID", IntegerType()),
                            StructField("Start_junction_road_name", StringType()),
                            StructField("End_junction_road_name", StringType()),
                            StructField("Latitude", DoubleType()),
                            StructField("Longitude", DoubleType()),
                            StructField("Link_length_km", DoubleType()),
                            StructField("Pedal_cycles", IntegerType()),
                            StructField("Two_wheeled_motor_vehicles", IntegerType()),
                            StructField("Cars_and_taxis", IntegerType()),
                            StructField("Buses_and_coaches", IntegerType()),
                            StructField("LGV_Type", IntegerType()),
                            StructField("HGV_Type", IntegerType()),
                            StructField("EV_Car", IntegerType()),
                            StructField("EV_Bike", IntegerType())
                        ])
    
    # read the data
    rawTraffic_Stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f'{checkpoint_path}/rawTrafficFolder/schemaInfer')
        .option("header", "true")
        .schema(schema)
        .load(landing_path+'/traffic/')
        .withColumn("Extract_Time", current_timestamp()))
        
    print("Reading traffic data is successful !!")
    # it returns a rawTraffic_Stream DF
    return rawTraffic_Stream

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating a write_traffic_data() Function

# COMMAND ----------

# write the df to table
def write_traffic_data(streamingDf, env):
    write_stream = (streamingDf.writeStream
            .format('delta')
            .option("checkpointLocation", checkpoint_path+'/rawTrafficFolder/checkpt')
            .outputMode('append')
            .queryName('rawTrafficWriteStream')
            .trigger(availableNow=True)
            .toTable(f"`{env}_catalog`.`bronze`.`raw_traffic`"))
    
    write_stream.awaitTermination()
    print("Writing traffic data is successful !!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating read_road_data() Function

# COMMAND ----------

def read_roads_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading Raw Roads data..... ")
    schema = StructType([StructField("Road_Id", IntegerType()),
                        StructField("Road_Category_Id", IntegerType()),
                        StructField("Road_Category", StringType()),
                        StructField("Region_Id", IntegerType()),
                        StructField("Region_Name", StringType()),
                        StructField("Total_Link_Length_Km",DoubleType()),
                        StructField("Total_Link_Length_Miles", DoubleType()),
                        StructField("All_Motor_Vehicles", DoubleType())
                    ])
    
    # read the data
    rawRoad_Stream = (spark.readStream
                            .format("cloudFiles")
                            .option("cloudFiles.format", "csv")
                            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/rawRoadsFolder/schemaInfer")
                            .schema(schema)
                            .load(landing_path+'/roads/')                   
                      )
    
    print("Reading Roads data is successful !!")

    # returns a df
    return rawRoad_Stream
                    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a write_road_data() Function

# COMMAND ----------

# write the df to table
def write_roads_data(streamingDf, env):
    write_stream = (streamingDf.writeStream
            .format('delta')
            .option("checkpointLocation", checkpoint_path+'/rawRoadsFolder/checkpt')
            .outputMode('append')
            .queryName('rawRoadWriteStream')
            .trigger(availableNow=True)
            .toTable(f"`{env}_catalog`.`bronze`.`raw_roads`"))
    
    write_stream.awaitTermination()
    print("Writing Road data is successful !!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling read and write functions

# COMMAND ----------

# Reading raw_traffic data from landing to bronze
read_traffic = read_traffic_data()

# Writing the raw_traffic data from landing to bronze
write_traffic_data(read_traffic, env)

# Reading raw_roads data from landing to bronze
read_roads = read_roads_data()

# Write the raw_roads data from landing to bronze
write_roads_data(read_roads, env)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Display data

# COMMAND ----------

display(spark.sql(f""" SELECT * FROM `{env}_catalog`.`bronze`.`raw_traffic` """))

# COMMAND ----------

display(spark.sql(f""" SELECT * FROM `{env}_catalog`.`bronze`.`raw_roads` """))

# COMMAND ----------

display(spark.sql(f""" SELECT count(*) FROM `{env}_catalog`.`bronze`.`raw_traffic` """))