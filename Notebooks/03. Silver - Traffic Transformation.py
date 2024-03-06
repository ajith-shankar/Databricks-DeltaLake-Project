# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Extract paths of Checkpoint, Bronze, Silver containers

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajiths0098@outlook.com/04. Common"

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read data from Bronze

# COMMAND ----------

def read_from_bronze(environment):
    print("Reading Bronze table")
    # Read data using spark structured streaming
    df_bronze_traffic  = ( spark.readStream
                                .table("`dev_catalog`.`bronze`.`raw_traffic`"))
    print(f"reading {environment}_catalog.bronze.raw_traffic is successful")

    return df_bronze_traffic

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Handling duplicate rows

# COMMAND ----------

def remove_duplicates(df):
    print("Removing Duplicate values : ", end='')
    df_dup = df.dropDuplicates()
    print("Success !!")
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Handling NULL values

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Getting count of Electric Vehicles by creating a new column

# COMMAND ----------

def ev_count(df):
    print("Creating Electric Vehicles Count column : ", end='')
    from pyspark.sql.functions import col
    df_ev = df.withColumn('Electric_Vehicle_Count', col('EV_Car') + col('EV_Bike'))
    print("Success !!")
    return df_ev

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Motor Vehile Count

# COMMAND ----------

def mv_count(df):
    print("Creating Motor Vehicles Count column : ", end='')
    from pyspark.sql.functions import col
    df_mv = df.withColumn('Motor_Vehicle_Count', col('Electric_Vehicle_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type'))
    print("Success !!")
    return df_mv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Transformed_Time column

# COMMAND ----------

def create_transformed_time(df):
  from pyspark.sql.functions import current_timestamp
  print("Creating Transformed_Time column : ", end='')
  df_time = df.withColumn('Transformed_Time', current_timestamp())
  print("Success !!")
  return df_time

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing the Transformed Data to Silver_Traffic table

# COMMAND ----------

def write_traffic_silver(StreamingDF, environment):
    print(f"Writing the {environment}_catalog.silver.silver_traffic data : ",end='')

    write_SilverStream = (StreamingDF.writeStream
                            .format('delta')
                            .option('checkpointLocation', checkpoint_path+'/SilverTrafficLoad/Checkpt')
                            .outputMode('append')
                            .queryName("SilverTrafficWriteStream")
                            .trigger(availableNow=True)
                            .toTable(f"`{environment}_catalog`.`silver`.`silver_traffic`")
                        )
    
    write_SilverStream.awaitTermination()
    print("Success !!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calling all the functions 

# COMMAND ----------

# Read Bronze table
df_traffic_data =  read_from_bronze(env)

# Removing Duplicates
df_dups = remove_duplicates(df_traffic_data)

# Replace any NULL values
allColumns = df_dups.schema.names
df_nulls = handle_nulls(df_dups, allColumns)

# Get EV counts
df_ev = ev_count(df_nulls)

# Get MV count
df_mv = mv_count(df_ev)

# Call Transformed_Time func
df_final = create_transformed_time(df_mv)

# Write to Silver tables
write_traffic_silver(df_final, env)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM `dev_catalog`.`silver`.`silver_traffic`