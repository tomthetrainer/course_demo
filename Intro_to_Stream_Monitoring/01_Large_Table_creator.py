# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Demo of performance characteristics for streams
# MAGIC
# MAGIC The goal of this notebook is to generate a series of tables to stream from.
# MAGIC
# MAGIC If you wanted to run this as a job, you would have to modify the schema-creation logic, I am not sure current_user() runs properly on a job cluster. 
# MAGIC
# MAGIC If you wanted to isolate the performance of the readers vs the performance of the writers you would run this notebook on a different cluster than the readers and writers notebook
# MAGIC
# MAGIC Note that this notebook manually generates some data tables. If you are looking for a more robust tool to create sample data, complete with parent/child relations. Check out dbldatagen here, https://github.com/databrickslabs/dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Create a database (schema) and use it
# MAGIC
# MAGIC 1. Build a string
# MAGIC 2. Create and use database

# COMMAND ----------

#####
# Extract the username, append a name to it
# And clean out special characters
#####
username = spark.sql("select current_user()").collect()[0][0]
#print(username)
database_name = f"{username}_intro_to_stream_monitoring"
#print(database_name)
database_name = (database_name.replace("+", "_").replace("@", "_").replace(".", "_"))
print(database_name)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create and use Database
# MAGIC
# MAGIC Note this tears down any previous tables in this database

# COMMAND ----------

spark.sql(f"Drop database if exists {database_name} cascade;")
spark.sql(f"Create database if not exists {database_name};")
spark.sql(f"use {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Verify Database and create some sample tables to stream from

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_database()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If you wanted to make your schema public, run this
# MAGIC -- GRANT ALL PRIVILEGES on schema your_schema to users;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Very Basic Base table used as source to generate the child tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists input_table as select 1 id, now() as event_time;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from input_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # example of the data we will be importing in batches that scale up to 200 and hold steady at 200
# MAGIC
# MAGIC No real data is pulled from the input_table, columns are replaced in the select
# MAGIC
# MAGIC A random id between 0 and 100 is generated along with an event_time
# MAGIC
# MAGIC We will use the event_time as a way to demonstrate windowed aggregations and streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC select floor(rand()*100) as id, now() as event_time from input_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- id STRING GENERATED ALWAYS AS (CAST('No ID' AS STRING)),
# MAGIC create table login_events (id long, event_time timestamp, event_type string)
# MAGIC --event_type GENERATED ALWAYS AS (CAST("login" AS STRING))
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from login_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table error_events (id long, event_time timestamp, event_type string)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table late_error_events (id long, event_time timestamp, event_type string)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table late_login_events (id long, event_time timestamp, event_type string)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE input_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert into login_events (select floor(rand()*100) as id, now() as event_time, "login" as event_type from input_table limit 200);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from login_events;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Table Creation Loop
# MAGIC
# MAGIC The loop is self explanatory but here is a summary
# MAGIC
# MAGIC input_table grows by 200 records every iteration. Random id(1>100), and current time for event_time
# MAGIC
# MAGIC login_events, grows by 200 every iteration, random id(1->100) and current time for event_time, event_type = "login"
# MAGIC
# MAGIC error_events, grows by 200 every iteration, random id(1->100) and current time for event_time, event_type = "error"
# MAGIC
# MAGIC late_error_events, triggered 5% of the time, grows by 200, random id(1->100) and current time - 1 day for event_time, event_type = "error"
# MAGIC
# MAGIC late_login_events, triggered 5% of the time, grows by 200, random id(1->100) and current time - 1 day for event_time, event_type = "login"

# COMMAND ----------

import time
import random
sc.setJobGroup("**TABLE CREATOR**", "***TABLE CREATOR***")
# range 1440 and sleep 5 should be about 2 hours
for i in range(1440):
    (spark.sql("insert into input_table  (select floor(rand()*100) as id, now() as event_time from input_table limit 200)"))
    (spark.sql("insert into login_events  (select floor(rand()*100) as id, now() as event_time, 'login' as event_type from input_table limit 200)"))
    (spark.sql("insert into error_events  (select floor(rand()*100) as id, now() as event_time, 'error' as event_type from input_table limit 200)"))
    ## Late data by one day, 5% of the time
    if (random.randint(0,100)>95):
        (spark.sql("insert into late_login_events  (select floor(rand()*100) as id, now() - interval 1 day as event_time, 'login' as event_type from input_table limit 200)"))
        print("late Login Data")
    else:
        (spark.sql("insert into late_login_events  (select floor(rand()*100) as id, now() as event_time, 'login' as event_type from input_table limit 200)"))
    if (random.randint(0,100)>95):
        (spark.sql("insert into late_error_events  (select floor(rand()*100) as id, now() - interval 1 day as event_time, 'error' as event_type from input_table limit 200)"))
        print("late Error Data")
    else:
        (spark.sql("insert into late_error_events  (select floor(rand()*100) as id, now() as event_time, 'error' as event_type from input_table limit 200)"))    

    print(f"running iteration {i}")
    time.sleep(5)

# COMMAND ----------


